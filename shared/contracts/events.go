// Package contracts tem os payloads de negócio que trafegam pelo gateway. A
// fonte da verdade continua sendo os JSON Schemas; estes tipos só mantêm
// produtores e consumidores alinhados em tempo de compilação.
package contracts

import "time"

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}
type OrderCreated struct {
	OrderID     string `json:"order_id"`
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
	Items       []Item `json:"items"`
}
type OrderCancelled struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}
type PaymentProcessed struct {
	PaymentID   string    `json:"payment_id"`
	OrderID     string    `json:"order_id"`
	AmountCents int64     `json:"amount_cents"`
	ChargedAt   time.Time `json:"charged_at"`
}
type PaymentFailed struct {
	PaymentID string `json:"payment_id"`
	OrderID   string `json:"order_id"`
	Reason    string `json:"reason"`
}
type PaymentRefunded struct {
	PaymentID   string    `json:"payment_id"`
	OrderID     string    `json:"order_id"`
	AmountCents int64     `json:"amount_cents"`
	RefundedAt  time.Time `json:"refunded_at"`
}
type StockReserved struct {
	ReservationID string `json:"reservation_id"`
	OrderID       string `json:"order_id"`
	Items         []Item `json:"items"`
}
type StockFailed struct {
	OrderID         string   `json:"order_id"`
	Reason          string   `json:"reason"`
	UnavailableSKUs []string `json:"unavailable_skus,omitempty"`
}
type StockReleased struct {
	ReservationID string    `json:"reservation_id"`
	OrderID       string    `json:"order_id"`
	ReleasedAt    time.Time `json:"released_at"`
}
type OrderFulfilled struct {
	FulfillmentID string    `json:"fulfillment_id"`
	OrderID       string    `json:"order_id"`
	DispatchedAt  time.Time `json:"dispatched_at"`
}
