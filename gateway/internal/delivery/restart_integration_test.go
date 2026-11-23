package delivery_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/shared/outbox"
)

// TestSiblingSuccessSurvivesWorkerRestart — dois assinantes do mesmo evento,
// falha em apenas um e reinício do worker de despacho no meio da recuperação.
// O sucesso do primeiro destino fica registrado (SENT + event_log DISPATCHED)
// e não é reenviado pelo novo worker; apenas o destino que falhou é repetido.
func TestSiblingSuccessSurvivesWorkerRestart(t *testing.T) {
	pool, teardown := startHookPool(t)
	defer teardown()

	var successHits, failingHits int64
	var failingHealthy atomic.Bool
	success := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&successHits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer success.Close()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&failingHits, 1)
		if failingHealthy.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failing.Close()

	eventID := uuid.New()
	seedDispatchPendingAt(t, pool, eventID, "successful-service", success.URL)
	seedDispatchPendingAt(t, pool, eventID, "failing-service", failing.URL)

	// Primeira instância do worker: entrega ao destino saudável e falha no outro.
	first, _, _ := spawnDispatchWorker(t, pool, http.DefaultClient, func(cfg *outbox.Config) {
		cfg.BackoffSchedule = outbox.BackoffExponential(time.Second)
	})
	ctx1, stop1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() { _ = first.Run(ctx1); close(done1) }()

	waitForStatus(t, pool, eventID, "successful-service", "SENT", 10*time.Second)
	require.Eventually(t, func() bool {
		var status string
		var retries int
		err := pool.QueryRow(context.Background(), `
			SELECT status,retry_count FROM dispatch_outbox
			WHERE event_id=$1 AND service_id='failing-service'`, eventID).Scan(&status, &retries)
		return err == nil && status == "PENDING" && retries >= 1
	}, 10*time.Second, 50*time.Millisecond)

	// "Reinício": a primeira instância para e uma nova assume a mesma tabela.
	stop1()
	select {
	case <-done1:
	case <-time.After(10 * time.Second):
		t.Fatal("first worker did not stop")
	}
	failingHealthy.Store(true)

	second, _, _ := spawnDispatchWorker(t, pool, http.DefaultClient, func(cfg *outbox.Config) {
		cfg.BackoffSchedule = outbox.BackoffExponential(time.Second)
	})
	ctx2, stop2 := context.WithCancel(context.Background())
	defer stop2()
	go func() { _ = second.Run(ctx2) }()

	waitForStatus(t, pool, eventID, "failing-service", "SENT", 20*time.Second)

	require.EqualValues(t, 1, atomic.LoadInt64(&successHits), "destino saudável não pode ser reenviado após o reinício")
	require.GreaterOrEqual(t, atomic.LoadInt64(&failingHits), int64(2))
	for _, service := range []string{"successful-service", "failing-service"} {
		var dispatched int
		require.NoError(t, pool.QueryRow(context.Background(), `
			SELECT count(*) FROM event_log
			WHERE event_id=$1 AND service_id=$2 AND direction='DISPATCHED'`, eventID, service).Scan(&dispatched))
		require.Equal(t, 1, dispatched, "event_log DISPATCHED para %s", service)
	}
	var dead int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM dead_letter WHERE event_id=$1`, eventID).Scan(&dead))
	require.Zero(t, dead)
}
