package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/victorvcruz/event-gateway/gateway/pkg/dispatcher"
	"github.com/victorvcruz/event-gateway/gateway/pkg/serviceoutbox"
	"github.com/victorvcruz/event-gateway/shared/contracts"
	"github.com/victorvcruz/event-gateway/shared/failpoint"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

type App struct {
	pool       *pgxpool.Pool
	outbox     *outbox.Client
	log        *zap.Logger
	operations *prometheus.CounterVec
}

type CreateRequest struct {
	CustomerID  string           `json:"customer_id"`
	AmountCents int64            `json:"amount_cents"`
	Items       []contracts.Item `json:"items"`
}
type Order struct {
	OrderID       string           `json:"order_id"`
	CorrelationID string           `json:"correlation_id"`
	CustomerID    string           `json:"customer_id"`
	AmountCents   int64            `json:"amount_cents"`
	Items         []contracts.Item `json:"items"`
	Status        string           `json:"status"`
	CancelReason  string           `json:"cancel_reason,omitempty"`
	CreatedAt     time.Time        `json:"created_at"`
	UpdatedAt     time.Time        `json:"updated_at"`
}

func New(pool *pgxpool.Pool, log *zap.Logger, operations *prometheus.CounterVec) *App {
	if operations == nil {
		operations = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "business_domain_operations_total", Help: "Business domain operations."}, []string{"service_id", "operation", "outcome"})
	}
	return &App{pool: pool, outbox: outbox.NewClient(pool, outbox.DefaultTable), log: log, operations: operations}
}

func (a *App) Dispatcher(opts ...dispatcher.Option) *dispatcher.Dispatcher {
	opts = append([]dispatcher.Option{dispatcher.WithLogger(a.log)}, opts...)
	d := dispatcher.New(opts...)
	d.Handle("payment.failed", a.handlePaymentFailed)
	d.Handle("stock.failed", a.handleStockFailed)
	d.Handle("order.fulfilled", a.handleFulfilled)
	return d
}

func (a *App) OrdersHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", a.postOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	return mux
}

func (a *App) postOrder(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 255 {
		http.Error(w, "Idempotency-Key is required and must be <= 255 characters", http.StatusBadRequest)
		return
	}
	var req CreateRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil || requireJSONEOF(dec) != nil || req.CustomerID == "" || req.AmountCents < 1 || !validItems(req.Items) {
		http.Error(w, "invalid order", http.StatusBadRequest)
		return
	}
	order, created, err := a.create(r.Context(), key, req)
	if err != nil {
		a.log.Error("create order", zap.Error(err))
		http.Error(w, "internal error", 500)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusAccepted
	}
	failpoint.Hit(r.Context(), failpoint.Checkpoint{
		Point:         failpoint.OrderHTTPAfterCommit,
		CorrelationID: order.CorrelationID,
		EventType:     "order.created",
	})
	writeJSON(w, code, map[string]string{"order_id": order.OrderID, "correlation_id": order.CorrelationID, "status": order.Status})
}

func requireJSONEOF(dec *json.Decoder) error {
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func validItems(items []contracts.Item) bool {
	if len(items) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(items))
	for _, v := range items {
		if strings.TrimSpace(v.SKU) == "" || v.Quantity < 1 {
			return false
		}
		if _, duplicate := seen[v.SKU]; duplicate {
			return false
		}
		seen[v.SKU] = struct{}{}
	}
	return true
}

func (a *App) create(ctx context.Context, key string, req CreateRequest) (Order, bool, error) {
	if existing, err := a.lookupByKey(ctx, key); err == nil {
		return existing, false, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return Order{}, false, err
	}
	id := uuid.New()
	corr := id.String()
	items, _ := json.Marshal(req.Items)
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return Order{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var inserted uuid.UUID
	err = tx.QueryRow(ctx, `INSERT INTO orders(order_id,idempotency_key,correlation_id,customer_id,amount_cents,items,status) VALUES($1,$2,$3,$4,$5,$6,'CREATED') ON CONFLICT(idempotency_key) DO NOTHING RETURNING order_id`, id, key, corr, req.CustomerID, req.AmountCents, items).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		o, e := a.lookupByKey(ctx, key)
		return o, false, e
	}
	if err != nil {
		return Order{}, false, err
	}
	row, err := serviceoutbox.NewRow("order.created", corr, contracts.OrderCreated{OrderID: id.String(), CustomerID: req.CustomerID, AmountCents: req.AmountCents, Items: req.Items})
	if err != nil {
		return Order{}, false, err
	}
	if err = a.outbox.InsertPending(ctx, tx, row); err != nil {
		return Order{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Order{}, false, err
	}
	a.operations.WithLabelValues("order-service", "create", "success").Inc()
	o, err := a.lookupByKey(ctx, key)
	return o, true, err
}

func (a *App) lookupByKey(ctx context.Context, key string) (Order, error) {
	return a.scan(a.pool.QueryRow(ctx, `SELECT order_id,correlation_id,customer_id,amount_cents,items,status,COALESCE(cancel_reason,''),created_at,updated_at FROM orders WHERE idempotency_key=$1`, key))
}
func (a *App) getOrder(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		http.Error(w, "invalid order id", 400)
		return
	}
	o, err := a.scan(a.pool.QueryRow(r.Context(), `SELECT order_id,correlation_id,customer_id,amount_cents,items,status,COALESCE(cancel_reason,''),created_at,updated_at FROM orders WHERE order_id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "not found", 404)
		return
	}
	if err != nil {
		http.Error(w, "internal error", 500)
		return
	}
	writeJSON(w, 200, o)
}

type scanner interface{ Scan(...any) error }

func (a *App) scan(s scanner) (Order, error) {
	var o Order
	var raw []byte
	if err := s.Scan(&o.OrderID, &o.CorrelationID, &o.CustomerID, &o.AmountCents, &raw, &o.Status, &o.CancelReason, &o.CreatedAt, &o.UpdatedAt); err != nil {
		return o, err
	}
	return o, json.Unmarshal(raw, &o.Items)
}

func (a *App) handlePaymentFailed(ctx context.Context, e dispatcher.Event) error {
	var p contracts.PaymentFailed
	if contracts.DecodePayload(e.Payload, &p) != nil || contracts.ValidatePaymentFailed(p) != nil {
		return dispatcher.ErrBadRequest
	}
	return a.cancel(ctx, p.OrderID, "payment_failed", e.CorrelationID)
}
func (a *App) handleStockFailed(ctx context.Context, e dispatcher.Event) error {
	var p contracts.StockFailed
	if contracts.DecodePayload(e.Payload, &p) != nil || contracts.ValidateStockFailed(p) != nil {
		return dispatcher.ErrBadRequest
	}
	return a.cancel(ctx, p.OrderID, "stock_failed", e.CorrelationID)
}
func (a *App) cancel(ctx context.Context, orderID, reason, corr string) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	if err = tx.QueryRow(ctx, `SELECT status FROM orders WHERE order_id=$1 FOR UPDATE`, orderID).Scan(&status); err != nil {
		return err
	}
	if status != "CREATED" {
		return dispatcher.ErrDuplicate
	}
	if _, err = tx.Exec(ctx, `UPDATE orders SET status='CANCELLED',cancel_reason=$2,updated_at=now() WHERE order_id=$1`, orderID, reason); err != nil {
		return err
	}
	row, err := serviceoutbox.NewRow("order.cancelled", corr, contracts.OrderCancelled{OrderID: orderID, Reason: reason})
	if err != nil {
		return err
	}
	if err = a.outbox.InsertPending(ctx, tx, row); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (a *App) handleFulfilled(ctx context.Context, e dispatcher.Event) error {
	var p contracts.OrderFulfilled
	if contracts.DecodePayload(e.Payload, &p) != nil || contracts.ValidateOrderFulfilled(p) != nil {
		return dispatcher.ErrBadRequest
	}
	tag, err := a.pool.Exec(ctx, `UPDATE orders SET status='FULFILLED',updated_at=now() WHERE order_id=$1 AND status='CREATED'`, p.OrderID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return dispatcher.ErrDuplicate
	}
	return nil
}
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

var _ = fmt.Sprintf
