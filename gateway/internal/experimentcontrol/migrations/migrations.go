package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS control_orders (
  order_id        UUID PRIMARY KEY,
  event_id        UUID NOT NULL UNIQUE,
  idempotency_key VARCHAR(255) NOT NULL UNIQUE,
  run_id          VARCHAR(255) NOT NULL,
  customer_id     VARCHAR(255) NOT NULL,
  amount_cents    BIGINT NOT NULL CHECK (amount_cents > 0),
  items           JSONB NOT NULL,
  event_timestamp TIMESTAMPTZ NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_control_orders_run ON control_orders (run_id, created_at);
`

func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("apply experiment control schema: %w", err)
	}
	return nil
}
