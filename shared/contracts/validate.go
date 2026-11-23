package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
)

var ErrInvalidPayload = errors.New("contracts: invalid payload")

// DecodePayload decodifica de forma estrita, como os JSON Schemas esperam:
// campo desconhecido ou JSON sobrando na borda do serviço é rejeitado.
func DecodePayload(raw []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("%w: trailing JSON", ErrInvalidPayload)
	}
	return nil
}

func validateUUID(field, value string) error {
	if _, err := uuid.Parse(value); err != nil {
		return fmt.Errorf("%w: %s must be a UUID", ErrInvalidPayload, field)
	}
	return nil
}

func ValidateItems(items []Item) error {
	if len(items) == 0 {
		return fmt.Errorf("%w: items must not be empty", ErrInvalidPayload)
	}
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if strings.TrimSpace(item.SKU) == "" {
			return fmt.Errorf("%w: sku must not be empty", ErrInvalidPayload)
		}
		if item.Quantity < 1 {
			return fmt.Errorf("%w: quantity must be positive", ErrInvalidPayload)
		}
		if _, exists := seen[item.SKU]; exists {
			return fmt.Errorf("%w: duplicate sku %q", ErrInvalidPayload, item.SKU)
		}
		seen[item.SKU] = struct{}{}
	}
	return nil
}

func ValidateOrderCreated(p OrderCreated) error {
	if err := validateUUID("order_id", p.OrderID); err != nil {
		return err
	}
	if strings.TrimSpace(p.CustomerID) == "" || p.AmountCents < 1 {
		return fmt.Errorf("%w: customer_id and positive amount_cents are required", ErrInvalidPayload)
	}
	return ValidateItems(p.Items)
}

func ValidateOrderCancelled(p OrderCancelled) error {
	if err := validateUUID("order_id", p.OrderID); err != nil {
		return err
	}
	switch p.Reason {
	case "payment_failed", "stock_failed", "customer_request":
		return nil
	default:
		return fmt.Errorf("%w: invalid cancellation reason", ErrInvalidPayload)
	}
}

func ValidatePaymentProcessed(p PaymentProcessed) error {
	if err := validateUUID("payment_id", p.PaymentID); err != nil {
		return err
	}
	if err := validateUUID("order_id", p.OrderID); err != nil {
		return err
	}
	if p.AmountCents < 1 || p.ChargedAt.IsZero() {
		return fmt.Errorf("%w: amount_cents and charged_at are required", ErrInvalidPayload)
	}
	return nil
}

func ValidatePaymentFailed(p PaymentFailed) error {
	if err := validateUUID("payment_id", p.PaymentID); err != nil {
		return err
	}
	if err := validateUUID("order_id", p.OrderID); err != nil {
		return err
	}
	if strings.TrimSpace(p.Reason) == "" {
		return fmt.Errorf("%w: reason is required", ErrInvalidPayload)
	}
	return nil
}

func ValidatePaymentRefunded(p PaymentRefunded) error {
	if err := validateUUID("payment_id", p.PaymentID); err != nil {
		return err
	}
	if err := validateUUID("order_id", p.OrderID); err != nil {
		return err
	}
	if p.AmountCents < 1 || p.RefundedAt.IsZero() {
		return fmt.Errorf("%w: amount_cents and refunded_at are required", ErrInvalidPayload)
	}
	return nil
}

func ValidateStockReserved(p StockReserved) error {
	if err := validateUUID("reservation_id", p.ReservationID); err != nil {
		return err
	}
	if err := validateUUID("order_id", p.OrderID); err != nil {
		return err
	}
	return ValidateItems(p.Items)
}

func ValidateStockFailed(p StockFailed) error {
	if err := validateUUID("order_id", p.OrderID); err != nil {
		return err
	}
	if strings.TrimSpace(p.Reason) == "" {
		return fmt.Errorf("%w: reason is required", ErrInvalidPayload)
	}
	for _, sku := range p.UnavailableSKUs {
		if strings.TrimSpace(sku) == "" {
			return fmt.Errorf("%w: unavailable sku must not be empty", ErrInvalidPayload)
		}
	}
	return nil
}

func ValidateStockReleased(p StockReleased) error {
	if err := validateUUID("reservation_id", p.ReservationID); err != nil {
		return err
	}
	if err := validateUUID("order_id", p.OrderID); err != nil {
		return err
	}
	if p.ReleasedAt.IsZero() {
		return fmt.Errorf("%w: released_at is required", ErrInvalidPayload)
	}
	return nil
}

func ValidateOrderFulfilled(p OrderFulfilled) error {
	if err := validateUUID("fulfillment_id", p.FulfillmentID); err != nil {
		return err
	}
	if err := validateUUID("order_id", p.OrderID); err != nil {
		return err
	}
	if p.DispatchedAt.IsZero() {
		return fmt.Errorf("%w: dispatched_at is required", ErrInvalidPayload)
	}
	return nil
}
