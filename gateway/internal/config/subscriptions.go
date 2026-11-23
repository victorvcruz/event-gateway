package config

import "github.com/victorvcruz/event-gateway/gateway/internal/consumer"

// SubscriptionRegistry adapta a config do YAML à interface
// consumer.SubscriptionRegistry. O mapa é montado no boot e não é relido.
type SubscriptionRegistry struct {
	byEventType map[string][]consumer.Subscriber
}

func NewSubscriptionRegistry(rules []SubscriptionRule) *SubscriptionRegistry {
	m := make(map[string][]consumer.Subscriber, len(rules))
	for _, r := range rules {
		subs := make([]consumer.Subscriber, 0, len(r.Consumers))
		for _, c := range r.Consumers {
			subs = append(subs, consumer.Subscriber{ID: c.ID, CallbackURL: c.URL})
		}
		m[r.EventType] = subs
	}
	return &SubscriptionRegistry{byEventType: m}
}

func (r *SubscriptionRegistry) SubscriptionsOf(eventType string) []consumer.Subscriber {
	return r.byEventType[eventType]
}

// EventTypes lista todos os event_type registrados; o consumer usa a lista
// para saber em quais tópicos se inscrever.
func (r *SubscriptionRegistry) EventTypes() []string {
	out := make([]string, 0, len(r.byEventType))
	for t := range r.byEventType {
		out = append(out, t)
	}
	return out
}
