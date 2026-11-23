package outbox

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Variáveis de ambiente para ampliar a política de tentativas de um worker sem
// recompilar. Existem para a execução suplementar do Experimento C. Sem elas,
// vale a política padrão de quem chama.
const (
	// EnvMaxAttempts substitui Config.MaxRetries (limite de chamadas).
	EnvMaxAttempts = "OUTBOX_MAX_ATTEMPTS"
	// EnvMaxBackoff limita o intervalo entre tentativas (ex.: "30s").
	EnvMaxBackoff = "OUTBOX_MAX_BACKOFF"
)

// RetryPolicyFromEnv devolve o limite de tentativas e o cronograma de espera.
// Sem as variáveis, são defaultAttempts e BackoffExponential(base). Com
// EnvMaxBackoff, usa BackoffExponentialCapped(base, teto).
func RetryPolicyFromEnv(defaultAttempts int, base time.Duration) (int, BackoffSchedule, error) {
	attempts := defaultAttempts
	if raw := os.Getenv(EnvMaxAttempts); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return 0, nil, fmt.Errorf("%s=%q: esperado inteiro >= 1", EnvMaxAttempts, raw)
		}
		attempts = parsed
	}
	schedule := BackoffExponential(base)
	if raw := os.Getenv(EnvMaxBackoff); raw != "" {
		limit, err := time.ParseDuration(raw)
		if err != nil || limit <= 0 {
			return 0, nil, fmt.Errorf("%s=%q: esperado duração positiva", EnvMaxBackoff, raw)
		}
		schedule = BackoffExponentialCapped(base, limit)
	}
	return attempts, schedule, nil
}
