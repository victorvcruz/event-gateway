package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DefaultTable é a tabela criada pelas migrations de shared/outbox. Os
	// testes podem passar outro nome ao NewClient para se isolarem.
	DefaultTable = "outbox"

	pgUniqueViolation = "23505"
)

// Client concentra o SQL do padrão outbox, para o gateway e os serviços de
// negócio não duplicarem esse código. Usa o *pgxpool.Pool quando precisa abrir
// transações próprias e aceita um pgx.Tx quando a operação faz parte de uma
// transação maior (como outbox + event_log na ingestão).
type Client struct {
	pool              *pgxpool.Pool
	table             string
	deliveryKeyColumn string
}

type ClientOption func(*Client)

// WithServiceIDDeliveryKey configura a identidade composta do dispatch_outbox,
// onde cada subscriber tem a sua própria entrega.
func WithServiceIDDeliveryKey() ClientOption {
	return func(c *Client) {
		c.deliveryKeyColumn = "service_id"
	}
}

// NewClient cria um Client sobre o pool. Com table vazio, usa DefaultTable.
func NewClient(pool *pgxpool.Pool, table string, opts ...ClientOption) *Client {
	if table == "" {
		table = DefaultTable
	}
	c := &Client{pool: pool, table: table}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Table devolve o nome da tabela do cliente, para quem precisa olhar a mesma
// tabela (como o gauge do Prometheus).
func (c *Client) Table() string { return c.table }

// Pool expõe o pool para quem precisa abrir a própria transação (o Worker.Run
// faz SELECT e UPDATE numa tx só).
func (c *Client) Pool() *pgxpool.Pool { return c.pool }

// BeginTx é atalho para pool.BeginTx com as opções padrão.
func (c *Client) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return c.pool.BeginTx(ctx, pgx.TxOptions{})
}

// InsertPending insere a row como PENDING dentro da tx. Devolve ErrDuplicate
// se a PRIMARY KEY já existir.
func (c *Client) InsertPending(ctx context.Context, tx pgx.Tx, row Row) error {
	_, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (event_id, event_type, payload, correlation_id, status)
		VALUES ($1, $2, $3, $4, 'PENDING')
	`, c.table),
		row.EventID, row.EventType, []byte(row.Payload), row.CorrelationID,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return ErrDuplicate
		}
		return err
	}
	return nil
}

// LookupStatus devolve o status atual do eventID, dentro da tx.
func (c *Client) LookupStatus(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (string, error) {
	var status string
	err := tx.QueryRow(ctx,
		fmt.Sprintf(`SELECT status FROM %s WHERE event_id=$1`, c.table), eventID,
	).Scan(&status)
	return status, err
}

// LookupStatusPool devolve o status usando o pool, fora de qualquer tx. É
// necessário depois de uma tx abortada: num conflito de PK o Postgres bloqueia
// as queries seguintes até o ROLLBACK, então a consulta precisa rodar fora dela.
func (c *Client) LookupStatusPool(ctx context.Context, eventID uuid.UUID) (string, error) {
	var status string
	err := c.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT status FROM %s WHERE event_id=$1`, c.table), eventID,
	).Scan(&status)
	return status, err
}

// MarkSent muda a linha para SENT e grava sent_at.
func (c *Client) MarkSent(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, sentAt time.Time) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET status='SENT', sent_at=$2, last_error=NULL WHERE event_id=$1`, c.table),
		eventID, sentAt,
	)
	return err
}

// MarkRowSent usa a identidade completa da entrega quando o client tem coluna
// de delivery key, para o ACK de um subscriber não marcar como SENT a entrega
// de outro.
func (c *Client) MarkRowSent(ctx context.Context, tx pgx.Tx, row Row, sentAt time.Time) error {
	if c.deliveryKeyColumn == "" {
		return c.MarkSent(ctx, tx, row.EventID, sentAt)
	}
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET status='SENT', sent_at=$2, last_error=NULL WHERE event_id=$1 AND %s=$3`, c.table, c.deliveryKeyColumn),
		row.EventID, sentAt, row.DeliveryKey,
	)
	return err
}

// IncrementRetry soma 1 ao retry_count e mantém a linha PENDING.
func (c *Client) IncrementRetry(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, retryCount int) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET retry_count=$2 WHERE event_id=$1`, c.table),
		eventID, retryCount,
	)
	return err
}

func (c *Client) IncrementRowRetry(ctx context.Context, tx pgx.Tx, row Row, retryCount int) error {
	if c.deliveryKeyColumn == "" {
		return c.IncrementRetry(ctx, tx, row.EventID, retryCount)
	}
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET retry_count=$2 WHERE event_id=$1 AND %s=$3`, c.table, c.deliveryKeyColumn),
		row.EventID, retryCount, row.DeliveryKey,
	)
	return err
}

// MarkDead muda a linha para DEAD e grava o retry_count final.
func (c *Client) MarkDead(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, retryCount int) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET status='DEAD', retry_count=$2 WHERE event_id=$1`, c.table),
		eventID, retryCount,
	)
	return err
}

// RecordDead grava também a causa da falha definitiva, além de mudar o estado.
func (c *Client) RecordDead(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, retryCount int, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET status='DEAD', retry_count=$2, last_error=$3 WHERE event_id=$1`, c.table),
		eventID, retryCount, message,
	)
	return err
}

func (c *Client) RecordRowDead(ctx context.Context, tx pgx.Tx, row Row, retryCount int, cause error) error {
	if c.deliveryKeyColumn == "" {
		return c.RecordDead(ctx, tx, row.EventID, retryCount, cause)
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET status='DEAD', retry_count=$2, last_error=$3 WHERE event_id=$1 AND %s=$4`, c.table, c.deliveryKeyColumn),
		row.EventID, retryCount, message, row.DeliveryKey,
	)
	return err
}

// SelectPending lê até batchSize linhas PENDING com FOR UPDATE SKIP LOCKED.
// Chame dentro de uma tx: o lock só é liberado no COMMIT ou ROLLBACK.
func (c *Client) SelectPending(ctx context.Context, tx pgx.Tx, batchSize int) ([]Row, error) {
	columns := c.selectColumns()
	query := fmt.Sprintf(`
		SELECT %s
		FROM %s
		WHERE status = 'PENDING'
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, columns, c.table)
	return c.scanRows(ctx, tx, query, batchSize)
}

// SelectPendingReady é como SelectPending, mas só traz linhas com
// next_attempt_at <= now(). O Worker a usa quando há BackoffSchedule, e a
// tabela precisa ter a coluna next_attempt_at.
func (c *Client) SelectPendingReady(ctx context.Context, tx pgx.Tx, batchSize int) ([]Row, error) {
	columns := c.selectColumns()
	query := fmt.Sprintf(`
		SELECT %s
		FROM %s
		WHERE status = 'PENDING' AND next_attempt_at <= now()
		ORDER BY next_attempt_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, columns, c.table)
	return c.scanRows(ctx, tx, query, batchSize)
}

// IncrementRetryWithBackoff é como IncrementRetry, mas também atualiza
// next_attempt_at. A tabela precisa ter essa coluna.
func (c *Client) IncrementRetryWithBackoff(ctx context.Context, tx pgx.Tx,
	eventID uuid.UUID, retryCount int, nextAttemptAt time.Time,
) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET retry_count=$2, next_attempt_at=$3 WHERE event_id=$1`, c.table),
		eventID, retryCount, nextAttemptAt,
	)
	return err
}

// RecordRetryWithBackoff grava o próximo horário de tentativa e a última causa de falha.
func (c *Client) RecordRetryWithBackoff(ctx context.Context, tx pgx.Tx,
	eventID uuid.UUID, retryCount int, nextAttemptAt time.Time, cause error,
) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET retry_count=$2, next_attempt_at=$3, last_error=$4 WHERE event_id=$1`, c.table),
		eventID, retryCount, nextAttemptAt, message,
	)
	return err
}

func (c *Client) RecordRowRetryWithBackoff(ctx context.Context, tx pgx.Tx,
	row Row, retryCount int, nextAttemptAt time.Time, cause error,
) error {
	if c.deliveryKeyColumn == "" {
		return c.RecordRetryWithBackoff(ctx, tx, row.EventID, retryCount, nextAttemptAt, cause)
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET retry_count=$2, next_attempt_at=$3, last_error=$4 WHERE event_id=$1 AND %s=$5`, c.table, c.deliveryKeyColumn),
		row.EventID, retryCount, nextAttemptAt, message, row.DeliveryKey,
	)
	return err
}

func (c *Client) selectColumns() string {
	columns := "event_id, event_type, payload, correlation_id, retry_count, COALESCE(last_error, '')"
	if c.deliveryKeyColumn != "" {
		columns += ", " + c.deliveryKeyColumn
	}
	return columns
}

func (c *Client) scanRows(ctx context.Context, tx pgx.Tx, query string, batchSize int) ([]Row, error) {
	rs, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	var out []Row
	for rs.Next() {
		var (
			r        Row
			payload  []byte
			corrNull *string
		)
		dest := []any{&r.EventID, &r.EventType, &payload, &corrNull, &r.RetryCount, &r.LastError}
		if c.deliveryKeyColumn != "" {
			dest = append(dest, &r.DeliveryKey)
		}
		if err := rs.Scan(dest...); err != nil {
			return nil, err
		}
		r.Payload = json.RawMessage(payload)
		if corrNull != nil {
			r.CorrelationID = *corrNull
		}
		out = append(out, r)
	}
	return out, rs.Err()
}
