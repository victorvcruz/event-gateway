package outbox_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/victorvcruz/event-gateway/shared/outbox"
)

func TestBackoffExponentialCapped(t *testing.T) {
	schedule := outbox.BackoffExponentialCapped(time.Second, 5*time.Second)

	assert.Equal(t, time.Second, schedule(0))
	assert.Equal(t, 2*time.Second, schedule(1))
	assert.Equal(t, 4*time.Second, schedule(2))
	assert.Equal(t, 5*time.Second, schedule(3))
	assert.Equal(t, 5*time.Second, schedule(100), "large retry counts must not overflow the cap")
}
