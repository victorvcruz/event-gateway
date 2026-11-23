// Package outbox define os contratos do padrão outbox usados pelo gateway
// (com Sender Kafka) e pelos dispatchers dos serviços (com Sender HTTP). O loop
// em si está em worker.go.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/victorvcruz/event-gateway/shared/failpoint"
)

//go:generate mockgen -source=$GOFILE -destination=./mocks/mock_outbox.go -package=mocks

type Row struct {
	EventID uuid.UUID
	// DeliveryKey distingue as várias entregas de um mesmo evento. Fica vazio
	// nos outboxes comuns e é o service_id no dispatch_outbox.
	DeliveryKey   string
	EventType     string
	CorrelationID string
	Payload       json.RawMessage
	RetryCount    int
	LastError     string
}

type Sender interface {
	Send(ctx context.Context, row Row) error
}

// DeadLetterHook é chamado pelo Worker quando o retry_count chega a MaxRetries
// ou o Sender devolve um erro com ErrNonRetryable. Ele recebe a tx do Worker,
// que já segura o lock da linha, e por isso pode marcar a linha como DEAD e
// inserir em dead_letter de forma atômica e sem deadlock. Se devolver nil, o
// Worker considera o evento tratado e não chama MarkDead. Se devolver erro, o
// Worker chama MarkDead como alternativa.
type DeadLetterHook func(ctx context.Context, tx pgx.Tx, row Row, cause error) error

// SuccessHook é chamado pelo Worker logo antes de commitar o UPDATE para SENT,
// na mesma tx. Se devolver erro, o Worker faz rollback e a linha continua
// PENDING, para ser reenviada no próximo ciclo. Serve para gravar efeitos
// junto com o SENT (como o event_log DISPATCHED). É opcional.
type SuccessHook func(ctx context.Context, tx pgx.Tx, row Row) error

// BackoffSchedule converte o retry_count (a partir de 0, antes de incrementar)
// no intervalo até next_attempt_at. É opcional: sem ele, a linha volta a
// PENDING na hora e o próximo ciclo a pega. Com ele, o SELECT do Worker filtra
// por next_attempt_at <= now() e a falha atualiza a coluna, que precisa existir
// na tabela.
type BackoffSchedule func(retryCount int) time.Duration

// BackoffExponential devolve um BackoffSchedule em que a espera é
// base * 2^retryCount. Com base de 1s, dá 1s, 2s, 4s, 8s, 16s...
func BackoffExponential(base time.Duration) BackoffSchedule {
	return func(retry int) time.Duration {
		if retry < 0 {
			retry = 0
		}
		return base * time.Duration(1<<retry)
	}
}

// BackoffExponentialCapped é o backoff exponencial limitado a max. Com max
// menor ou igual a zero, não há limite.
func BackoffExponentialCapped(base, max time.Duration) BackoffSchedule {
	return func(retry int) time.Duration {
		if retry < 0 {
			retry = 0
		}
		delay := base
		const largestDuration = time.Duration(1<<63 - 1)
		for range retry {
			if max > 0 && delay >= max {
				return max
			}
			if delay > largestDuration/2 {
				if max > 0 {
					return max
				}
				return largestDuration
			}
			delay *= 2
		}
		if max > 0 && delay > max {
			return max
		}
		return delay
	}
}

type Config struct {
	Table        string
	BatchSize    int
	PollInterval time.Duration
	// MaxRetries, apesar do nome, é o total de chamadas a Send, contando a
	// primeira. Com 6, são uma tentativa inicial e cinco retries.
	MaxRetries      int
	DeadLetter      DeadLetterHook
	Success         SuccessHook
	BackoffSchedule BackoffSchedule
	Probe           failpoint.Probe
}

var ErrInvalidConfig = errors.New("outbox: invalid config")

func (c Config) Validate() error {
	if c.Table == "" {
		return errors.Join(ErrInvalidConfig, errors.New("Table is required"))
	}
	if c.BatchSize <= 0 {
		return errors.Join(ErrInvalidConfig, errors.New("BatchSize must be > 0"))
	}
	if c.PollInterval <= 0 {
		return errors.Join(ErrInvalidConfig, errors.New("PollInterval must be > 0"))
	}
	if c.MaxRetries < 0 {
		return errors.Join(ErrInvalidConfig, errors.New("MaxRetries must be >= 0"))
	}
	return nil
}
