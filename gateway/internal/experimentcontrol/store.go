package experimentcontrol

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store interface {
	Create(context.Context, Order) (Order, bool, error)
	Ping(context.Context) error
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *PostgresStore) Create(ctx context.Context, order Order) (Order, bool, error) {
	items, err := json.Marshal(order.Items)
	if err != nil {
		return Order{}, false, fmt.Errorf("marshal items: %w", err)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Order{}, false, fmt.Errorf("begin control order: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO control_orders (
			order_id, event_id, idempotency_key, run_id, customer_id,
			amount_cents, items, event_timestamp
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, order.OrderID, order.EventID, order.IdempotencyKey, order.RunID,
		order.CustomerID, order.AmountCents, items, order.EventTimestamp)
	if err != nil {
		return Order{}, false, fmt.Errorf("insert control order: %w", err)
	}
	created := tag.RowsAffected() == 1
	if !created {
		var rawItems []byte
		err = tx.QueryRow(ctx, `
			SELECT order_id,event_id,idempotency_key,run_id,customer_id,
			       amount_cents,items,event_timestamp
			FROM control_orders WHERE idempotency_key=$1
		`, order.IdempotencyKey).Scan(
			&order.OrderID, &order.EventID, &order.IdempotencyKey, &order.RunID,
			&order.CustomerID, &order.AmountCents, &rawItems, &order.EventTimestamp,
		)
		if err != nil {
			return Order{}, false, fmt.Errorf("lookup duplicate control order: %w", err)
		}
		if err := json.Unmarshal(rawItems, &order.Items); err != nil {
			return Order{}, false, fmt.Errorf("decode stored items: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Order{}, false, fmt.Errorf("commit control order: %w", err)
	}
	return order, created, nil
}
