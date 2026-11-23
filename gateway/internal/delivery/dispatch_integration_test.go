package delivery_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/internal/delivery"
	"github.com/victorvcruz/event-gateway/gateway/internal/repository"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

// spawnDispatchWorker sobe um outbox.Worker configurado para dispatch_outbox
// com o HTTPSender injetado + SuccessHook + DeadLetterSink.
func spawnDispatchWorker(t *testing.T, pool *pgxpool.Pool, doer delivery.HTTPDoer,
	extraOpts ...func(*outbox.Config),
) (*outbox.Worker, *prometheus.HistogramVec, *prometheus.CounterVec) {
	t.Helper()
	client := outbox.NewClient(pool, "dispatch_outbox", outbox.WithServiceIDDeliveryKey())
	sender := delivery.NewHTTPSender(doer)
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "test_dispatch_latency_seconds",
	}, []string{"event_type", "service_id"})
	deadTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_dispatch_dead_letter_total",
	}, []string{"event_type", "service_id"})

	dispatch := repository.NewDispatch(pool)
	successHook := delivery.NewSuccessHookEventLog(dispatch, latency)
	deadLetterRepo := repository.NewDeadLetter(pool)
	sink := delivery.NewDeadLetterSink(
		client, deadLetterRepo,
		prometheus.NewCounterVec(prometheus.CounterOpts{Name: "unused"}, []string{"event_type"}),
		delivery.ServiceIDKafkaBroker,
	).WithDispatch(dispatch, deadTotal)

	cfg := outbox.Config{
		Table:           "dispatch_outbox",
		BatchSize:       10,
		PollInterval:    100 * time.Millisecond,
		MaxRetries:      5,
		BackoffSchedule: outbox.BackoffExponential(200 * time.Millisecond),
		Success:         successHook.Handle,
		DeadLetter:      sink.Handle,
	}
	for _, opt := range extraOpts {
		opt(&cfg)
	}

	w, err := outbox.NewWorker(client, sender, cfg, zap.NewNop())
	require.NoError(t, err)
	return w, latency, deadTotal
}

func waitForStatus(t *testing.T, pool *pgxpool.Pool, eventID uuid.UUID,
	serviceID, wantStatus string, timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		err := pool.QueryRow(context.Background(), `
			SELECT status FROM dispatch_outbox
			WHERE event_id=$1 AND service_id=$2`, eventID, serviceID,
		).Scan(&status)
		if err == nil && status == wantStatus {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	var status string
	_ = pool.QueryRow(context.Background(), `
		SELECT status FROM dispatch_outbox
		WHERE event_id=$1 AND service_id=$2`, eventID, serviceID).Scan(&status)
	t.Fatalf("status nunca chegou a %q (último=%q, event_id=%s)",
		wantStatus, status, eventID)
}

// TestHappyPathEndToEnd: linha PENDING chega ao subscriber e termina SENT.
func TestHappyPathEndToEnd(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	var (
		received int64
		lastReq  *http.Request
		lastBody []byte
	)
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&received, 1)
		lastReq = r.Clone(r.Context())
		lastBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	defer stub.Close()

	eventID := uuid.New()
	row := seedDispatchPendingAt(t, pool, eventID, "payment-service", stub.URL)

	w, _, _ := spawnDispatchWorker(t, pool, http.DefaultClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	waitForStatus(t, pool, eventID, "payment-service", "SENT", 10*time.Second)

	// exatamente 1 hit no subscriber; body é o envelope raw
	assert.EqualValues(t, 1, atomic.LoadInt64(&received))
	require.NotNil(t, lastReq)
	assert.Equal(t, "saga-e2e", lastReq.Header.Get("X-Correlation-Id"))
	deliveryID := lastReq.Header.Get("X-Gateway-Delivery-Id")
	_, uuidErr := uuid.Parse(deliveryID)
	assert.NoError(t, uuidErr, "delivery-id deve ser UUID")

	var envRaw map[string]any
	require.NoError(t, json.Unmarshal(lastBody, &envRaw))
	assert.Equal(t, "order.created", envRaw["event_type"])
	_ = row

	// event_log(DISPATCHED) presente + sent_at NOT NULL
	var direction, serviceID string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT direction, service_id FROM event_log
		WHERE event_id=$1 AND direction='DISPATCHED'`, eventID,
	).Scan(&direction, &serviceID))
	assert.Equal(t, "payment-service", serviceID)

	var sentAt *time.Time
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT sent_at FROM dispatch_outbox WHERE event_id=$1 AND service_id=$2`,
		eventID, "payment-service").Scan(&sentAt))
	require.NotNil(t, sentAt)
}

func TestSiblingSubscriberACKDoesNotMaskRetryableFailure(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	success := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer success.Close()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failing.Close()

	eventID := uuid.New()
	seedDispatchPendingAt(t, pool, eventID, "successful-service", success.URL)
	seedDispatchPendingAt(t, pool, eventID, "failing-service", failing.URL)

	w, _, _ := spawnDispatchWorker(t, pool, http.DefaultClient, func(cfg *outbox.Config) {
		cfg.BackoffSchedule = outbox.BackoffExponential(5 * time.Second)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	waitForStatus(t, pool, eventID, "successful-service", "SENT", 10*time.Second)
	require.Eventually(t, func() bool {
		var status string
		var retries int
		err := pool.QueryRow(context.Background(), `
			SELECT status,retry_count FROM dispatch_outbox
			WHERE event_id=$1 AND service_id='failing-service'`, eventID).Scan(&status, &retries)
		return err == nil && status == "PENDING" && retries == 1
	}, 10*time.Second, 50*time.Millisecond)
}

// TestSubscriber409TreatedAsAck: se o subscriber responde 409 de cara, o
// gateway trata como sucesso (SENT + event_log DISPATCHED, sem retry). É isso
// que sustenta a semântica effectively-once.
func TestSubscriber409TreatedAsAck(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	var attempts int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&attempts, 1)
		w.WriteHeader(409)
	}))
	defer stub.Close()

	eventID := uuid.New()
	_ = seedDispatchPendingAt(t, pool, eventID, "payment-service", stub.URL)

	w, _, _ := spawnDispatchWorker(t, pool, http.DefaultClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	waitForStatus(t, pool, eventID, "payment-service", "SENT", 10*time.Second)

	assert.EqualValues(t, 1, atomic.LoadInt64(&attempts),
		"409 é ACK — nenhum retry")

	var retry int
	var sentAt *time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT retry_count, sent_at FROM dispatch_outbox
		WHERE event_id=$1 AND service_id=$2`, eventID, "payment-service",
	).Scan(&retry, &sentAt))
	assert.Equal(t, 0, retry, "retry_count zero — nunca falhou do ponto de vista do Worker")
	require.NotNil(t, sentAt, "sent_at populado no MarkSent")

	var direction, svc string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT direction, service_id FROM event_log
		WHERE event_id=$1 AND direction='DISPATCHED'`, eventID,
	).Scan(&direction, &svc))
	assert.Equal(t, "DISPATCHED", direction)
	assert.Equal(t, "payment-service", svc)

	// nenhum dead_letter — 409 nunca cai nesse caminho
	var dl int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM dead_letter WHERE event_id=$1`, eventID).Scan(&dl))
	assert.Equal(t, 0, dl)
}

// TestSubscriberRecoversAfterKFailures: o subscriber falha K vezes com 500 e
// depois responde 200. A linha termina SENT com retry_count=K, e o tempo total
// respeita o backoff exponencial.
func TestSubscriberRecoversAfterKFailures(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	const failsBeforeSuccess = 3

	var attempts int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&attempts, 1)
		if n <= failsBeforeSuccess {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	}))
	defer stub.Close()

	eventID := uuid.New()
	_ = seedDispatchPendingAt(t, pool, eventID, "payment-service", stub.URL)

	// Base curta (200ms) para o teste terminar em <5s:
	// falha 0 → next_attempt = now+200ms
	// falha 1 → next_attempt = now+400ms
	// falha 2 → next_attempt = now+800ms
	// tentativa 3 → SENT
	// tempo mínimo entre INSERT e SENT ≈ 200+400+800 = 1400ms
	const backoffBase = 200 * time.Millisecond
	w, _, _ := spawnDispatchWorker(t, pool, http.DefaultClient, func(cfg *outbox.Config) {
		cfg.BackoffSchedule = outbox.BackoffExponential(backoffBase)
		cfg.PollInterval = 50 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	start := time.Now()
	waitForStatus(t, pool, eventID, "payment-service", "SENT", 15*time.Second)
	elapsed := time.Since(start)

	assert.EqualValues(t, failsBeforeSuccess+1, atomic.LoadInt64(&attempts),
		"exatamente K+1 tentativas")

	// tempo total >= soma do backoff das K falhas
	minExpected := backoffBase + backoffBase*2 + backoffBase*4
	assert.GreaterOrEqual(t, elapsed, minExpected,
		"tempo total precisa respeitar backoff exponencial (min %v)", minExpected)

	// retry_count final = K
	var retry int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT retry_count FROM dispatch_outbox
		WHERE event_id=$1 AND service_id=$2`, eventID, "payment-service").Scan(&retry))
	assert.Equal(t, failsBeforeSuccess, retry)

	// exatamente 1 linha em event_log(DISPATCHED)
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT count(*) FROM event_log
		WHERE event_id=$1 AND direction='DISPATCHED'`, eventID).Scan(&n))
	assert.Equal(t, 1, n, "SuccessHook grava exatamente uma vez (na tentativa bem-sucedida)")
}

// TestTwoWorkersProcessExactlyOnce: com 100 linhas PENDING e dois
// outbox.Worker em paralelo, o subscriber recebe exatamente 100 POSTs. O FOR
// UPDATE SKIP LOCKED evita a entrega dupla.
func TestTwoWorkersProcessExactlyOnce(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	const numEvents = 100
	var received int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&received, 1)
		w.WriteHeader(200)
	}))
	defer stub.Close()

	// Seed 100 linhas com service_ids distintos para evitar colisão de PK
	// composta entre as próprias linhas (aqui simulamos 100 eventos que já
	// passaram pelo Consumer).
	eventIDs := make([]uuid.UUID, numEvents)
	for i := range numEvents {
		id := uuid.New()
		eventIDs[i] = id
		serviceID := "svc-" + id.String()[:8]
		_ = seedDispatchPendingAt(t, pool, id, serviceID, stub.URL)
	}

	// Dois workers idênticos compartilhando o pool.
	worker1, _, _ := spawnDispatchWorker(t, pool, http.DefaultClient, func(cfg *outbox.Config) {
		cfg.BackoffSchedule = outbox.BackoffExponential(50 * time.Millisecond)
		cfg.PollInterval = 25 * time.Millisecond
		cfg.BatchSize = 20
	})
	worker2, _, _ := spawnDispatchWorker(t, pool, http.DefaultClient, func(cfg *outbox.Config) {
		cfg.BackoffSchedule = outbox.BackoffExponential(50 * time.Millisecond)
		cfg.PollInterval = 25 * time.Millisecond
		cfg.BatchSize = 20
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = worker1.Run(ctx) }()
	go func() { defer wg.Done(); _ = worker2.Run(ctx) }()

	// Aguarda até que TODAS estejam SENT (ou timeout do context).
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		var pending int
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT count(*) FROM dispatch_outbox WHERE status='PENDING'`).Scan(&pending))
		if pending == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cancel()
	wg.Wait()

	// Após shutdown: exatamente 100 hits (sem duplicação por dupla-entrega).
	total := atomic.LoadInt64(&received)
	assert.EqualValues(t, numEvents, total,
		"FOR UPDATE SKIP LOCKED garante exatamente %d POSTs (contagem final: %d)",
		numEvents, total)

	// Confirmação estrutural: todas as linhas SENT, nenhuma PENDING/DEAD.
	var sent, pending, dead int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM dispatch_outbox WHERE status='SENT'`).Scan(&sent))
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM dispatch_outbox WHERE status='PENDING'`).Scan(&pending))
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM dispatch_outbox WHERE status='DEAD'`).Scan(&dead))
	assert.Equal(t, numEvents, sent)
	assert.Equal(t, 0, pending)
	assert.Equal(t, 0, dead)
}

// TestHeadersAcrossRetries: em 3 tentativas (2 falhas e 1 sucesso), o
// X-Correlation-Id e o event_id do body se repetem, e os três
// X-Gateway-Delivery-Id são diferentes, cada um um UUIDv4.
func TestHeadersAcrossRetries(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	type observed struct {
		correlationID string
		deliveryID    string
		bodyEventID   string
	}
	var (
		mu    sync.Mutex
		obs   []observed
		count int64
	)

	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(bodyBytes, &body)
		bodyID, _ := body["event_id"].(string)

		mu.Lock()
		obs = append(obs, observed{
			correlationID: r.Header.Get("X-Correlation-Id"),
			deliveryID:    r.Header.Get("X-Gateway-Delivery-Id"),
			bodyEventID:   bodyID,
		})
		mu.Unlock()

		n := atomic.AddInt64(&count, 1)
		if n <= 2 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	}))
	defer stub.Close()

	eventID := uuid.New()
	_ = seedDispatchPendingAt(t, pool, eventID, "payment-service", stub.URL)

	w, _, _ := spawnDispatchWorker(t, pool, http.DefaultClient, func(cfg *outbox.Config) {
		cfg.BackoffSchedule = outbox.BackoffExponential(100 * time.Millisecond)
		cfg.PollInterval = 25 * time.Millisecond
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	waitForStatus(t, pool, eventID, "payment-service", "SENT", 15*time.Second)

	mu.Lock()
	captured := append([]observed(nil), obs...)
	mu.Unlock()

	require.Len(t, captured, 3, "esperado exatamente 3 tentativas")

	// mesmo correlation_id em todas as tentativas (constante entre retries)
	assert.Equal(t, "saga-e2e", captured[0].correlationID)
	assert.Equal(t, captured[0].correlationID, captured[1].correlationID)
	assert.Equal(t, captured[1].correlationID, captured[2].correlationID)

	// mesmo event_id no body em todas as tentativas
	assert.Equal(t, eventID.String(), captured[0].bodyEventID)
	assert.Equal(t, captured[0].bodyEventID, captured[1].bodyEventID)
	assert.Equal(t, captured[1].bodyEventID, captured[2].bodyEventID)

	// três delivery-ids distintos, todos UUIDv4
	ids := []string{captured[0].deliveryID, captured[1].deliveryID, captured[2].deliveryID}
	seen := map[string]bool{}
	for _, id := range ids {
		assert.False(t, seen[id], "delivery-id %s repetido entre tentativas", id)
		seen[id] = true
		parsed, err := uuid.Parse(id)
		require.NoError(t, err, "delivery-id %s deve ser UUID", id)
		assert.Equal(t, uuid.Version(4), parsed.Version(), "UUIDv4 esperado")
	}
}

// TestDispatchDeadLetterAfterMaxRetries: com o subscriber sempre em 500, a
// linha vira DEAD ao fim de MaxRetries. O dead_letter ganha uma linha com o
// service_id certo e failure_reason preenchido, e o ciclo seguinte não
// reprocessa.
func TestDispatchDeadLetterAfterMaxRetries(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	var attempts int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&attempts, 1)
		w.WriteHeader(500)
	}))
	defer stub.Close()

	eventID := uuid.New()
	_ = seedDispatchPendingAt(t, pool, eventID, "payment-service", stub.URL)

	// MaxRetries=5 → 5 tentativas totais. Base=50ms → 50+100+200+400=750ms
	// de espera cumulativa entre tentativas.
	w, _, deadTotal := spawnDispatchWorker(t, pool, http.DefaultClient, func(cfg *outbox.Config) {
		cfg.BackoffSchedule = outbox.BackoffExponential(50 * time.Millisecond)
		cfg.PollInterval = 25 * time.Millisecond
		cfg.MaxRetries = 5
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	waitForStatus(t, pool, eventID, "payment-service", "DEAD", 15*time.Second)

	assert.EqualValues(t, 5, atomic.LoadInt64(&attempts),
		"MaxRetries=5 → exatamente 5 tentativas HTTP")

	// dead_letter tem 1 linha com service_id correto
	var svc, reason string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT service_id, failure_reason FROM dead_letter
		WHERE event_id=$1`, eventID).Scan(&svc, &reason))
	assert.Equal(t, "payment-service", svc,
		"service_id vem do wrapper (não do serviceID do construtor)")
	assert.NotEmpty(t, reason, "failure_reason NOT NULL")
	assert.LessOrEqual(t, len(reason), 4000,
		"failure_reason truncado em 4000 chars (mesma regra do relay)")

	// dispatch_outbox tem retry_count final
	var retry int
	var lastError string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT retry_count, last_error FROM dispatch_outbox
		WHERE event_id=$1 AND service_id=$2`, eventID, "payment-service").Scan(&retry, &lastError))
	assert.Equal(t, 5, retry)
	assert.NotEmpty(t, lastError)

	// métrica
	assert.GreaterOrEqual(t,
		testCounterValue(t, deadTotal, "order.created", "payment-service"),
		float64(1), "gateway_dispatch_dead_letter_total incrementou")

	// nenhum event_log DISPATCHED (nunca teve sucesso)
	var dispatched int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT count(*) FROM event_log
		WHERE event_id=$1 AND direction='DISPATCHED'`, eventID).Scan(&dispatched))
	assert.Equal(t, 0, dispatched)

	// próximo ciclo NÃO reprocessa: aguardo 300ms adicionais e attempts continua = 5
	beforeSleep := atomic.LoadInt64(&attempts)
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, beforeSleep, atomic.LoadInt64(&attempts),
		"linha DEAD não é reselecionada pelo Worker")
}

// TestDispatchNonRetryable4xxImmediateDead: um 422 na primeira tentativa leva
// a linha a DEAD sem esgotar MaxRetries.
func TestDispatchNonRetryable4xxImmediateDead(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	var attempts int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&attempts, 1)
		w.WriteHeader(422)
		_, _ = w.Write([]byte("subscriber says: unprocessable"))
	}))
	defer stub.Close()

	eventID := uuid.New()
	_ = seedDispatchPendingAt(t, pool, eventID, "inventory-service", stub.URL)

	w, _, _ := spawnDispatchWorker(t, pool, http.DefaultClient, func(cfg *outbox.Config) {
		cfg.BackoffSchedule = outbox.BackoffExponential(200 * time.Millisecond)
		cfg.PollInterval = 25 * time.Millisecond
		cfg.MaxRetries = 5
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	waitForStatus(t, pool, eventID, "inventory-service", "DEAD", 5*time.Second)

	assert.EqualValues(t, 1, atomic.LoadInt64(&attempts),
		"ErrNonRetryable pula direto para DEAD; NUNCA retenta")

	// dead_letter tem 1 linha; failure_reason inclui body do subscriber
	var reason string
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT failure_reason FROM dead_letter WHERE event_id=$1`, eventID).Scan(&reason))
	assert.Contains(t, reason, "422")
	assert.Contains(t, reason, "unprocessable")

	// retry_count = 1 (uma tentativa falhada, não zero)
	var retry int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT retry_count FROM dispatch_outbox
		WHERE event_id=$1 AND service_id=$2`, eventID, "inventory-service").Scan(&retry))
	assert.Equal(t, 1, retry, "reflete a única tentativa falhada")
}

func testCounterValue(t *testing.T, cv *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, cv.WithLabelValues(labels...).Write(m))
	return m.GetCounter().GetValue()
}

// seedDispatchPendingAt é como seedDispatchPending mas com callback URL
// controlada pelo teste (para apontar ao httptest.Server).
func seedDispatchPendingAt(t *testing.T, pool *pgxpool.Pool,
	eventID uuid.UUID, serviceID, callbackURL string,
) outbox.Row {
	t.Helper()

	envelope := map[string]any{
		"event_id":       eventID,
		"event_type":     "order.created",
		"correlation_id": "saga-e2e",
		"payload":        map[string]any{"order_id": "42"},
		"occurred_at":    time.Now().UTC(),
	}
	envBytes, _ := json.Marshal(envelope)

	wrapper, err := delivery.EncodeDispatchEnvelope(
		delivery.TargetMeta{ServiceID: serviceID, CallbackURL: callbackURL},
		envBytes,
	)
	require.NoError(t, err)

	_, err = pool.Exec(context.Background(), `
		INSERT INTO dispatch_outbox
		  (event_id, service_id, callback_url, event_type, correlation_id,
		   payload, occurred_at, status, retry_count, next_attempt_at)
		VALUES ($1, $2, $3, 'order.created', 'saga-e2e', $4, now(),
		        'PENDING', 0, now())`,
		eventID, serviceID, callbackURL, wrapper,
	)
	require.NoError(t, err)
	return outbox.Row{
		EventID:       eventID,
		EventType:     "order.created",
		CorrelationID: "saga-e2e",
		Payload:       wrapper,
	}
}
