package failpoint

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type recordingProbe struct{ got []Checkpoint }

func (p *recordingProbe) Hit(_ context.Context, checkpoint Checkpoint) {
	p.got = append(p.got, checkpoint)
}

func TestContextHit(t *testing.T) {
	probe := &recordingProbe{}
	checkpoint := Checkpoint{Point: DispatchAfterHandlerCommit, CorrelationID: "order-1"}
	Hit(WithContext(context.Background(), probe), checkpoint)
	require.Equal(t, []Checkpoint{checkpoint}, probe.got)
}

func TestHitWithoutProbeIsNoop(t *testing.T) {
	require.NotPanics(t, func() { Hit(context.Background(), Checkpoint{Point: "unused"}) })
}

func TestMiddlewareInjectsProbe(t *testing.T) {
	probe := &recordingProbe{}
	handler := Middleware(probe, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		Hit(r.Context(), Checkpoint{Point: OrderHTTPAfterCommit})
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	require.Len(t, probe.got, 1)
}

func TestNewDefaultRuntimeIsSafe(t *testing.T) {
	require.NotPanics(t, func() {
		New("test", zap.NewNop()).Hit(context.Background(), Checkpoint{Point: "ignored"})
	})
}
