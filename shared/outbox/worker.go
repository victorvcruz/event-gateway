package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/shared/failpoint"
)

type Worker struct {
	client *Client
	sender Sender
	cfg    Config
	log    *zap.Logger
	clock  func() time.Time
}

// NewWorker monta um Worker sobre o client e o sender e valida a cfg. Se
// cfg.Table estiver preenchida, ela vale no lugar da tabela do client; senão,
// usa a do client.
func NewWorker(client *Client, sender Sender, cfg Config, log *zap.Logger) (*Worker, error) {
	if client == nil {
		return nil, fmt.Errorf("outbox: client is required")
	}
	if sender == nil {
		return nil, fmt.Errorf("outbox: sender is required")
	}
	if cfg.Table == "" {
		cfg.Table = client.Table()
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &Worker{
		client: client,
		sender: sender,
		cfg:    cfg,
		log:    log,
		clock:  time.Now,
	}, nil
}

// Run bloqueia até o ctx ser cancelado. A cada tick, abre uma transação,
// pega linhas PENDING com FOR UPDATE SKIP LOCKED, entrega pelo Sender e commita.
func (w *Worker) Run(ctx context.Context) error {
	t := time.NewTicker(w.cfg.PollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := w.tick(ctx); err != nil && ctx.Err() == nil {
				w.log.Warn("outbox worker tick failed", zap.Error(err))
			}
		}
	}
}

// Tick roda um único ciclo. Existe para os testes.
func (w *Worker) Tick(ctx context.Context) error { return w.tick(ctx) }

func (w *Worker) tick(ctx context.Context) error {
	tx, err := w.client.Pool().BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Se cfg.Table for diferente da tabela do client, cria um client
	// temporário para ela, e o SQL continua todo no Client.
	c := w.client
	if w.cfg.Table != c.Table() {
		c = NewClient(w.client.Pool(), w.cfg.Table)
	}

	var rows []Row
	if w.cfg.BackoffSchedule != nil {
		rows, err = c.SelectPendingReady(ctx, tx, w.cfg.BatchSize)
	} else {
		rows, err = c.SelectPending(ctx, tx, w.cfg.BatchSize)
	}
	if err != nil {
		return fmt.Errorf("select pending: %w", err)
	}

	for _, row := range rows {
		if err := w.processRow(ctx, tx, c, row); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// processRow faz o Send e a mudança de status. Só devolve erro quando a falha
// deve abortar a tx do Worker (por exemplo, o SuccessHook falhou e a linha
// precisa voltar a PENDING). Falhas de escrita não fatais são logadas e a tx
// continua.
func (w *Worker) processRow(ctx context.Context, tx pgx.Tx, c *Client, row Row) error {
	sendErr := w.sender.Send(ctx, row)
	if sendErr == nil {
		if w.cfg.Probe != nil {
			w.cfg.Probe.Hit(ctx, failpoint.Checkpoint{
				Point:         failpoint.OutboxAfterSendACK,
				EventID:       row.EventID.String(),
				EventType:     row.EventType,
				CorrelationID: row.CorrelationID,
				OutboxTable:   w.cfg.Table,
				DeliveryKey:   row.DeliveryKey,
			})
		}
		if w.cfg.Success != nil {
			if err := w.cfg.Success(ctx, tx, row); err != nil {
				w.log.Warn("success hook failed — aborting tx",
					zap.String("event_id", row.EventID.String()), zap.Error(err))
				return fmt.Errorf("success hook: %w", err)
			}
		}
		if err := c.MarkRowSent(ctx, tx, row, w.clock()); err != nil {
			w.log.Warn("mark sent failed", zap.String("event_id", row.EventID.String()), zap.Error(err))
		}
		return nil
	}

	nonRetryable := errors.Is(sendErr, ErrNonRetryable)
	next := row.RetryCount + 1

	if nonRetryable || next >= w.cfg.MaxRetries {
		row.RetryCount = next
		if w.cfg.DeadLetter != nil {
			if hookErr := w.cfg.DeadLetter(ctx, tx, row, sendErr); hookErr != nil {
				w.log.Warn("dead-letter hook failed",
					zap.String("event_id", row.EventID.String()), zap.Error(hookErr))
				if err := c.RecordRowDead(ctx, tx, row, next, sendErr); err != nil {
					w.log.Warn("mark dead fallback failed",
						zap.String("event_id", row.EventID.String()), zap.Error(err))
				}
			}
			return nil
		}
		if err := c.RecordRowDead(ctx, tx, row, next, sendErr); err != nil {
			w.log.Warn("mark dead failed",
				zap.String("event_id", row.EventID.String()), zap.Error(err))
		}
		return nil
	}

	if w.cfg.BackoffSchedule != nil {
		nextAt := w.clock().Add(w.cfg.BackoffSchedule(row.RetryCount))
		if err := c.RecordRowRetryWithBackoff(ctx, tx, row, next, nextAt, sendErr); err != nil {
			w.log.Warn("increment retry with backoff failed",
				zap.String("event_id", row.EventID.String()), zap.Error(err))
		}
		return nil
	}

	if err := c.IncrementRowRetry(ctx, tx, row, next); err != nil {
		w.log.Warn("increment retry failed",
			zap.String("event_id", row.EventID.String()), zap.Error(err))
	}
	return nil
}
