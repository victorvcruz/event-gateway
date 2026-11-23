package outbox

import (
	"testing"
	"time"
)

func TestRetryPolicyFromEnvKeepsDefaultWhenUnset(t *testing.T) {
	t.Setenv(EnvMaxAttempts, "")
	t.Setenv(EnvMaxBackoff, "")
	attempts, schedule, err := RetryPolicyFromEnv(6, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 6 {
		t.Fatalf("attempts = %d, want 6", attempts)
	}
	var total time.Duration
	for retry := 0; retry < attempts-1; retry++ {
		total += schedule(retry)
	}
	if total != 31*time.Second {
		t.Fatalf("default wait budget = %s, want 31s", total)
	}
}

func TestRetryPolicyFromEnvExtendsBudgetWithCap(t *testing.T) {
	t.Setenv(EnvMaxAttempts, "10")
	t.Setenv(EnvMaxBackoff, "30s")
	attempts, schedule, err := RetryPolicyFromEnv(6, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 10 {
		t.Fatalf("attempts = %d, want 10", attempts)
	}
	var total time.Duration
	for retry := 0; retry < attempts-1; retry++ {
		total += schedule(retry)
	}
	// 1+2+4+8+16 + 4*30 = 151 s de espera acumulada.
	if total != 151*time.Second {
		t.Fatalf("extended wait budget = %s, want 151s", total)
	}
}

func TestRetryPolicyFromEnvRejectsInvalidValues(t *testing.T) {
	for _, tc := range []struct{ name, attempts, backoff string }{
		{"attempts not a number", "dez", ""},
		{"attempts below one", "0", ""},
		{"backoff not a duration", "", "rapido"},
		{"backoff not positive", "", "-5s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EnvMaxAttempts, tc.attempts)
			t.Setenv(EnvMaxBackoff, tc.backoff)
			if _, _, err := RetryPolicyFromEnv(6, time.Second); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
