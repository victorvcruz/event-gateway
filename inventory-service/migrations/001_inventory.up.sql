CREATE TABLE inventory_items(sku VARCHAR(255) PRIMARY KEY,available INT NOT NULL CHECK(available>=0),updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
INSERT INTO inventory_items(sku,available)VALUES('SKU-001',100),('SKU-002',50)ON CONFLICT DO NOTHING;
CREATE TABLE reservations(reservation_id UUID PRIMARY KEY,order_id UUID NOT NULL UNIQUE,items JSONB NOT NULL,status VARCHAR(20) NOT NULL CHECK(status IN('ACTIVE','FAILED','RELEASED','SKIPPED')),failure_reason TEXT,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE release_requests(order_id UUID PRIMARY KEY,source_event_id UUID NOT NULL UNIQUE,requested_at TIMESTAMPTZ NOT NULL DEFAULT now());
