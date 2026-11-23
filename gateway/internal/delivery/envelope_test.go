package delivery_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/internal/delivery"
)

func TestEncodeDecodeRoundtrip(t *testing.T) {
	target := delivery.TargetMeta{
		ServiceID:   "payment-service",
		CallbackURL: "http://payment/dispatch",
	}
	env := json.RawMessage(`{"order_id":"6c8a1d7f","amount":9900}`)

	encoded, err := delivery.EncodeDispatchEnvelope(target, env)
	require.NoError(t, err)

	decoded, err := delivery.DecodeDispatchEnvelope(encoded)
	require.NoError(t, err)

	assert.Equal(t, target, decoded.Target)
	assert.JSONEq(t, string(env), string(decoded.Envelope))
}

func TestEncodeRejectsMissingFields(t *testing.T) {
	cases := []struct {
		name string
		t    delivery.TargetMeta
		env  json.RawMessage
	}{
		{"empty service_id", delivery.TargetMeta{CallbackURL: "http://x"}, json.RawMessage(`{}`)},
		{"empty callback_url", delivery.TargetMeta{ServiceID: "x"}, json.RawMessage(`{}`)},
		{"empty envelope", delivery.TargetMeta{ServiceID: "x", CallbackURL: "http://x"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := delivery.EncodeDispatchEnvelope(c.t, c.env)
			require.Error(t, err)
			assert.True(t, errors.Is(err, delivery.ErrInvalidWrapper))
		})
	}
}

func TestDecodeRejectsCorruptedPayload(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{"not json", []byte("not json at all")},
		{"empty", []byte("")},
		{"missing target", []byte(`{"envelope":{"x":1}}`)},
		{"empty service_id", []byte(`{"target":{"service_id":"","callback_url":"http://x"},"envelope":{"x":1}}`)},
		{"empty callback_url", []byte(`{"target":{"service_id":"x","callback_url":""},"envelope":{"x":1}}`)},
		{"empty envelope", []byte(`{"target":{"service_id":"x","callback_url":"http://x"}}`)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := delivery.DecodeDispatchEnvelope(c.in)
			require.Error(t, err)
			assert.True(t, errors.Is(err, delivery.ErrInvalidWrapper))
		})
	}
}

func TestEnvelopePreservedByteForByte(t *testing.T) {
	env := json.RawMessage(`{"a":1,"b":"str with \"quotes\"","c":[1,2,3]}`)
	encoded, err := delivery.EncodeDispatchEnvelope(
		delivery.TargetMeta{ServiceID: "s", CallbackURL: "http://x"},
		env,
	)
	require.NoError(t, err)

	decoded, err := delivery.DecodeDispatchEnvelope(encoded)
	require.NoError(t, err)
	assert.JSONEq(t, string(env), string(decoded.Envelope))
}
