package consumer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/victorvcruz/event-gateway/gateway/internal/config"
	"github.com/victorvcruz/event-gateway/gateway/internal/consumer"
)

// Confirma que config.SubscriptionRegistry satisfaz a interface do consumer.
var _ consumer.SubscriptionRegistry = (*config.SubscriptionRegistry)(nil)

func TestConfigSubscriptionRegistryMapsRules(t *testing.T) {
	rules := []config.SubscriptionRule{
		{
			EventType: "order.created",
			Consumers: []config.SubscriberConfig{
				{ID: "payment-service", URL: "http://payment/dispatch"},
				{ID: "inventory-service", URL: "http://inventory/dispatch"},
			},
		},
		{
			EventType: "payment.processed",
			Consumers: []config.SubscriberConfig{
				{ID: "fulfillment-service", URL: "http://fulfillment/dispatch"},
			},
		},
	}
	r := config.NewSubscriptionRegistry(rules)

	assert.ElementsMatch(t, []consumer.Subscriber{
		{ID: "payment-service", CallbackURL: "http://payment/dispatch"},
		{ID: "inventory-service", CallbackURL: "http://inventory/dispatch"},
	}, r.SubscriptionsOf("order.created"))

	assert.ElementsMatch(t, []consumer.Subscriber{
		{ID: "fulfillment-service", CallbackURL: "http://fulfillment/dispatch"},
	}, r.SubscriptionsOf("payment.processed"))

	assert.Empty(t, r.SubscriptionsOf("stock.reserved"),
		"event_type sem regra deve retornar slice vazio (nunca nil-crash)")

	assert.ElementsMatch(t, []string{"order.created", "payment.processed"}, r.EventTypes())
}
