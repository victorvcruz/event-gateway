CREATE TABLE orders (
  order_id UUID PRIMARY KEY,
  idempotency_key VARCHAR(255) NOT NULL UNIQUE,
  correlation_id VARCHAR(255) NOT NULL UNIQUE,
  customer_id VARCHAR(255) NOT NULL,
  amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
  items JSONB NOT NULL,
  status VARCHAR(20) NOT NULL CHECK (status IN ('CREATED','FULFILLED','CANCELLED')),
  cancel_reason VARCHAR(40),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
