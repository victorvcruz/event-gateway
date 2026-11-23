package logging_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/shared/logging"
)

func TestNewDefaults(t *testing.T) {
	log, err := logging.New(logging.Config{ServiceID: "gateway-server"})

	require.NoError(t, err)
	require.NotNil(t, log)
}

func TestNewInvalidLevel(t *testing.T) {
	_, err := logging.New(logging.Config{Level: "banana"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid log level")
}

func TestFromContextInjectsFields(t *testing.T) {
	base, err := logging.New(logging.Config{})
	require.NoError(t, err)

	ctx := context.Background()
	ctx = logging.WithEventID(ctx, "evt-123")
	ctx = logging.WithCorrelationID(ctx, "saga-abc")

	got := logging.FromContext(ctx, base)

	assert.NotSame(t, base, got, "expected a derived logger when ctx carries ids")
	_ = zap.String
}

func TestFromContextEmptyReturnsBase(t *testing.T) {
	base, err := logging.New(logging.Config{})
	require.NoError(t, err)

	got := logging.FromContext(context.Background(), base)

	assert.Same(t, base, got)
}

func TestContextGettersEmpty(t *testing.T) {
	_, ok := logging.EventID(context.Background())
	assert.False(t, ok)

	_, ok = logging.CorrelationID(context.Background())
	assert.False(t, ok)
}
