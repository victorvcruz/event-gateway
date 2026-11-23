package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/shared/outbox"
)

const testDDL = `
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE test_outbox (
  event_id        UUID         PRIMARY KEY,
  event_type      VARCHAR(255) NOT NULL,
  payload         JSONB        NOT NULL,
  correlation_id  VARCHAR(255),
  status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
                               CHECK (status IN ('PENDING', 'SENT', 'DEAD')),
  retry_count     INT          NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
  last_error      TEXT,
  created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
  sent_at         TIMESTAMPTZ
);
CREATE INDEX idx_outbox_pending ON test_outbox (created_at) WHERE status='PENDING';
CREATE INDEX idx_outbox_ready ON test_outbox (next_attempt_at) WHERE status='PENDING';
`

func startPool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("gateway_db"),
		tcpostgres.WithUsername("gateway"),
		tcpostgres.WithPassword("gateway"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)

	_, err = pool.Exec(context.Background(), testDDL)
	require.NoError(t, err)

	return pool, func() {
		pool.Close()
		_ = container.Terminate(context.Background())
	}
}

func seed(t *testing.T, pool *pgxpool.Pool, n int) []uuid.UUID {
	t.Helper()
	ctx := context.Background()
	ids := make([]uuid.UUID, n)
	for i := range n {
		ids[i] = uuid.New()
		_, err := pool.Exec(ctx, `
			INSERT INTO test_outbox (event_id, event_type, payload, correlation_id)
			VALUES ($1, 'order.created', $2, $3)
		`, ids[i], json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)), fmt.Sprintf("saga-%d", i))
		require.NoError(t, err)
	}
	return ids
}

type stubSender struct {
	sent atomic.Int64
	fail atomic.Int64 // number of times to fail before succeeding
	err  error
	mu   sync.Mutex
	seen []uuid.UUID
}

func (s *stubSender) Send(_ context.Context, row outbox.Row) error {
	s.sent.Add(1)
	s.mu.Lock()
	s.seen = append(s.seen, row.EventID)
	s.mu.Unlock()
	if s.fail.Load() > 0 {
		s.fail.Add(-1)
		return s.err
	}
	return nil
}

func newWorker(t *testing.T, pool *pgxpool.Pool, sender outbox.Sender, cfg outbox.Config) *outbox.Worker {
	t.Helper()
	if cfg.Table == "" {
		cfg.Table = "test_outbox"
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 10
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	client := outbox.NewClient(pool, cfg.Table)
	w, err := outbox.NewWorker(client, sender, cfg, zap.NewNop())
	require.NoError(t, err)
	return w
}

func TestWorkerHappyPath(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	ids := seed(t, pool, 3)
	sender := &stubSender{}
	w := newWorker(t, pool, sender, outbox.Config{})

	require.NoError(t, w.Tick(context.Background()))
	assert.EqualValues(t, 3, sender.sent.Load())

	var count int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM test_outbox WHERE status='SENT' AND sent_at IS NOT NULL`,
	).Scan(&count))
	assert.Equal(t, 3, count)

	// confere que todos os ids semeados foram marcados
	for _, id := range ids {
		var s string
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT status FROM test_outbox WHERE event_id=$1`, id,
		).Scan(&s))
		assert.Equal(t, "SENT", s)
	}
}

func TestWorkerRetriesThenDead(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	seed(t, pool, 1)
	sender := &stubSender{err: errors.New("boom")}
	sender.fail.Store(10) // always fail
	w := newWorker(t, pool, sender, outbox.Config{MaxRetries: 3})

	ctx := context.Background()
	// ticks 1 e 2 mantêm PENDING e incrementam retry_count
	// o tick 3 promove para DEAD (retry_count chega a MaxRetries)
	for range 3 {
		require.NoError(t, w.Tick(ctx))
	}

	var status string
	var retry int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status, retry_count FROM test_outbox`,
	).Scan(&status, &retry))
	assert.Equal(t, "DEAD", status)
	assert.GreaterOrEqual(t, retry, 3)
}

func TestWorkerDeadLetterHookFires(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	seed(t, pool, 1)
	sender := &stubSender{err: errors.New("boom")}
	sender.fail.Store(10)

	var hookCalls atomic.Int32
	client := outbox.NewClient(pool, "test_outbox")
	w := newWorker(t, pool, sender, outbox.Config{
		MaxRetries: 2,
		DeadLetter: func(ctx context.Context, tx pgx.Tx, row outbox.Row, _ error) error {
			hookCalls.Add(1)
			// Hook mimeta o DeadLetterSink real: marca DEAD na mesma tx.
			return client.MarkDead(ctx, tx, row.EventID, row.RetryCount)
		},
	})

	ctx := context.Background()
	for range 3 {
		require.NoError(t, w.Tick(ctx))
	}

	assert.EqualValues(t, 1, hookCalls.Load(), "hook must fire exactly once per event")

	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM test_outbox`,
	).Scan(&status))
	assert.Equal(t, "DEAD", status, "hook deve ter deixado a row em DEAD")
}

// TestWorkerRecoversBeforeMaxRetries — Sender falha K < MaxRetries vezes
// e depois recupera; row deve terminar em SENT sem invocar o hook.
func TestWorkerRecoversBeforeMaxRetries(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	seed(t, pool, 1)
	sender := &stubSender{err: errors.New("transient")}
	sender.fail.Store(2) // 2 falhas, depois sucesso

	var hookCalls atomic.Int32
	w := newWorker(t, pool, sender, outbox.Config{
		MaxRetries: 5,
		DeadLetter: func(_ context.Context, _ pgx.Tx, _ outbox.Row, _ error) error {
			hookCalls.Add(1)
			return nil
		},
	})

	ctx := context.Background()
	// tick 1: falha (retry=1). tick 2: falha (retry=2). tick 3: sucesso → SENT.
	for range 3 {
		require.NoError(t, w.Tick(ctx))
	}

	var status string
	var retry int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status, retry_count FROM test_outbox`,
	).Scan(&status, &retry))
	assert.Equal(t, "SENT", status, "após K<MaxRetries falhas + sucesso, row deve virar SENT")
	assert.EqualValues(t, 0, hookCalls.Load(), "hook NÃO é invocado quando o retry recupera")
}

func TestWorkerSelectSkipsLocked(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	seed(t, pool, 5)

	sender := &stubSender{}
	w := newWorker(t, pool, sender, outbox.Config{BatchSize: 2})

	require.NoError(t, w.Tick(context.Background()))
	assert.EqualValues(t, 2, sender.sent.Load(), "first tick claims up to BatchSize rows")

	require.NoError(t, w.Tick(context.Background()))
	assert.EqualValues(t, 4, sender.sent.Load())
}

func TestNewWorkerRejectsInvalidConfig(t *testing.T) {
	_, err := outbox.NewWorker(nil, nil, outbox.Config{}, zap.NewNop())
	require.Error(t, err)
}

// crashSender simula um Kafka producer que ACK a mensagem mas o processo
// morre antes do UPDATE status='SENT'. Cancela o ctx logo no primeiro
// Send — o worker vê Send=nil mas MarkSent falha e Commit também falha,
// deixando a row em PENDING para o próximo ciclo republicar.
type crashSender struct {
	cancel context.CancelFunc
	fired  atomic.Bool
}

func (c *crashSender) Send(_ context.Context, _ outbox.Row) error {
	if c.fired.CompareAndSwap(false, true) {
		c.cancel()
	}
	return nil
}

// TestWorkerCrashBeforeUpdate: um crash entre o ACK do Kafka e o UPDATE para
// SENT deixa a row em PENDING (o commit é abortado), e o próximo ciclo
// republica.
func TestWorkerCrashBeforeUpdate(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	ids := seed(t, pool, 1)
	id := ids[0]

	// Tick 1: sender ACK, cancela ctx → MarkSent falha, Commit falha → PENDING.
	ctxCrash, cancelCrash := context.WithCancel(context.Background())
	crash := &crashSender{cancel: cancelCrash}
	w1 := newWorker(t, pool, crash, outbox.Config{})

	_ = w1.Tick(ctxCrash) // erro esperado; a asserção é sobre o estado no DB.

	var status string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM test_outbox WHERE event_id=$1`, id,
	).Scan(&status))
	assert.Equal(t, "PENDING", status,
		"crash antes do commit deve manter row em PENDING")

	// Tick 2: sender normal + ctx fresco → row vai para SENT.
	normal := &stubSender{}
	w2 := newWorker(t, pool, normal, outbox.Config{})
	require.NoError(t, w2.Tick(context.Background()))

	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status, sent_at FROM test_outbox WHERE event_id=$1`, id,
	).Scan(&status, new(time.Time)))
	assert.Equal(t, "SENT", status, "próximo ciclo republica e marca SENT")
	assert.EqualValues(t, 1, normal.sent.Load(),
		"o evento é republicado exatamente uma vez após o crash")
}

// concurrencySender é uma variante thread-safe de stubSender — usada nos
// testes de concorrência para evitar race no append de seen.
type concurrencySender struct{ sent atomic.Int64 }

func (c *concurrencySender) Send(_ context.Context, _ outbox.Row) error {
	c.sent.Add(1)
	return nil
}

// TestWorkerConcurrency: dois Workers.Run disputam 100 rows PENDING. Graças ao
// SELECT FOR UPDATE SKIP LOCKED, o Sender é chamado exatamente 100 vezes.
func TestWorkerConcurrency(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	const n = 100
	seed(t, pool, n)

	sender := &concurrencySender{}
	w1 := newWorker(t, pool, sender, outbox.Config{BatchSize: 10, PollInterval: 20 * time.Millisecond})
	w2 := newWorker(t, pool, sender, outbox.Config{BatchSize: 10, PollInterval: 20 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = w1.Run(ctx) }()
	go func() { defer wg.Done(); _ = w2.Run(ctx) }()

	require.Eventually(t, func() bool {
		var count int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM test_outbox WHERE status='SENT'`,
		).Scan(&count); err != nil {
			return false
		}
		return count == n
	}, 10*time.Second, 50*time.Millisecond, "todas as 100 rows devem virar SENT")

	cancel()
	wg.Wait()

	assert.EqualValues(t, n, sender.sent.Load(),
		"FOR UPDATE SKIP LOCKED garante exatamente %d Send calls (sem duplicação)", n)

	var still int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM test_outbox WHERE status!='SENT'`,
	).Scan(&still))
	assert.Equal(t, 0, still, "nenhuma row deve permanecer PENDING/DEAD")
}

func TestWorkerBackoffScheduleGatesRetries(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	seed(t, pool, 1)
	sender := &stubSender{err: errors.New("temporary")}
	sender.fail.Store(1) // fails once, then succeeds

	w := newWorker(t, pool, sender, outbox.Config{
		MaxRetries:      5,
		BackoffSchedule: outbox.BackoffExponential(200 * time.Millisecond),
	})

	ctx := context.Background()
	require.NoError(t, w.Tick(ctx))
	assert.EqualValues(t, 1, sender.sent.Load(), "one failed send")

	var status string
	var retry int
	var nextAt time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status, retry_count, next_attempt_at FROM test_outbox`,
	).Scan(&status, &retry, &nextAt))
	assert.Equal(t, "PENDING", status)
	assert.Equal(t, 1, retry)
	assert.WithinDuration(t, time.Now().Add(200*time.Millisecond), nextAt, 100*time.Millisecond)

	require.NoError(t, w.Tick(ctx))
	assert.EqualValues(t, 1, sender.sent.Load(), "still gated by next_attempt_at")

	time.Sleep(250 * time.Millisecond)
	require.NoError(t, w.Tick(ctx))
	assert.EqualValues(t, 2, sender.sent.Load(), "gate elapsed, retried and succeeded")

	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM test_outbox`).Scan(&status))
	assert.Equal(t, "SENT", status)
}

func TestWorkerSuccessHookRunsInSameTx(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	_, err := pool.Exec(context.Background(),
		`CREATE TABLE side_effect (event_id UUID PRIMARY KEY)`)
	require.NoError(t, err)

	seed(t, pool, 2)
	sender := &stubSender{}

	hookCalled := atomic.Int64{}
	hook := func(ctx context.Context, tx pgx.Tx, row outbox.Row) error {
		hookCalled.Add(1)
		_, err := tx.Exec(ctx, `INSERT INTO side_effect (event_id) VALUES ($1)`, row.EventID)
		return err
	}

	w := newWorker(t, pool, sender, outbox.Config{Success: hook})
	require.NoError(t, w.Tick(context.Background()))

	assert.EqualValues(t, 2, hookCalled.Load())
	var count int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM side_effect`).Scan(&count))
	assert.Equal(t, 2, count, "hook rows visible after tx commit")
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM test_outbox WHERE status='SENT'`).Scan(&count))
	assert.Equal(t, 2, count, "SENT visible after tx commit")
}

func TestWorkerSuccessHookFailureAbortsTx(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	seed(t, pool, 1)
	sender := &stubSender{}
	failHook := func(_ context.Context, _ pgx.Tx, _ outbox.Row) error {
		return errors.New("hook boom")
	}

	w := newWorker(t, pool, sender, outbox.Config{Success: failHook})
	err := w.Tick(context.Background())
	require.Error(t, err, "hook failure should surface")

	var status string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM test_outbox`).Scan(&status))
	assert.Equal(t, "PENDING", status, "row must remain PENDING when hook aborts tx")
}

func TestWorkerErrNonRetryableSkipsRetryStraightToDead(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	seed(t, pool, 1)
	sender := &stubSender{err: fmt.Errorf("subscriber said 422: %w", outbox.ErrNonRetryable)}
	sender.fail.Store(1) // fail once — that's enough

	deadCalled := atomic.Int64{}
	deadHook := func(ctx context.Context, tx pgx.Tx, row outbox.Row, cause error) error {
		deadCalled.Add(1)
		assert.True(t, errors.Is(cause, outbox.ErrNonRetryable), "cause should carry ErrNonRetryable")
		return nil
	}

	w := newWorker(t, pool, sender, outbox.Config{
		MaxRetries: 5,
		DeadLetter: deadHook,
	})

	require.NoError(t, w.Tick(context.Background()))

	assert.EqualValues(t, 1, sender.sent.Load(), "single Send call")
	assert.EqualValues(t, 1, deadCalled.Load(),
		"DEAD path taken on first failure (would need MaxRetries=5 attempts without ErrNonRetryable)")
}

// TestWorkerConcurrencyWithBackoffSchedule garante que o SelectPendingReady
// (usado com BackoffSchedule) também respeita o FOR UPDATE SKIP LOCKED entre
// vários workers. Não repete o teste acima: aqui a query é a "ready", com
// WHERE next_attempt_at <= now().
func TestWorkerConcurrencyWithBackoffSchedule(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	const n = 100
	seed(t, pool, n)

	sender := &stubSender{}
	// dois workers idênticos com BackoffSchedule ativo
	cfg := outbox.Config{
		Table:           "test_outbox",
		BatchSize:       10,
		PollInterval:    20 * time.Millisecond,
		MaxRetries:      3,
		BackoffSchedule: outbox.BackoffExponential(100 * time.Millisecond),
	}
	w1 := newWorker(t, pool, sender, cfg)
	w2 := newWorker(t, pool, sender, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = w1.Run(ctx) }()
	go func() { defer wg.Done(); _ = w2.Run(ctx) }()

	// Aguarda até TODAS SENT
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var pending int
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT count(*) FROM test_outbox WHERE status!='SENT'`).Scan(&pending))
		if pending == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	wg.Wait()

	assert.EqualValues(t, n, sender.sent.Load(),
		"SelectPendingReady + SKIP LOCKED entrega cada linha exatamente 1×")
}

func TestWorkerErrNonRetryableWithoutHookFallsBackToMarkDead(t *testing.T) {
	pool, teardown := startPool(t)
	defer teardown()

	seed(t, pool, 1)
	sender := &stubSender{err: fmt.Errorf("boom: %w", outbox.ErrNonRetryable)}
	sender.fail.Store(1)

	w := newWorker(t, pool, sender, outbox.Config{MaxRetries: 5}) // no DeadLetter hook
	require.NoError(t, w.Tick(context.Background()))

	var status string
	var retry int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status, retry_count FROM test_outbox`).Scan(&status, &retry))
	assert.Equal(t, "DEAD", status, "fallback MarkDead taken when hook is nil")
	assert.Equal(t, 1, retry, "retry_count reflects the single failed attempt")
}
