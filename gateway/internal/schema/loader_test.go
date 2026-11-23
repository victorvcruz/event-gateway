package schema_test

import (
	"context"
	"encoding/json"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/victorvcruz/event-gateway/gateway/internal/schema"
)

const orderCreatedSchema = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "additionalProperties": false,
  "required": ["order_id", "customer_id", "amount"],
  "properties": {
    "order_id":    {"type": "integer"},
    "customer_id": {"type": "integer"},
    "amount":      {"type": "number"}
  }
}`

func fsWith(files map[string]string) fstest.MapFS {
	m := fstest.MapFS{}
	for name, body := range files {
		m["schemas/"+name] = &fstest.MapFile{Data: []byte(body)}
	}
	return m
}

func TestLoadFromEmbeddedFS(t *testing.T) {
	// Confirma que os schemas embarcados compilam sem erro.
	l := schema.NewLoader()
	require.NoError(t, l.Load(context.Background()))
}

func TestLoadValidSyntheticSchema(t *testing.T) {
	l := schema.NewLoaderFromFS(fsWith(map[string]string{
		"order.created.json": orderCreatedSchema,
	}), "schemas")
	require.NoError(t, l.Load(context.Background()))
}

func TestLoadBrokenJSONFailsFast(t *testing.T) {
	l := schema.NewLoaderFromFS(fsWith(map[string]string{
		"order.created.json": `{not valid`,
	}), "schemas")
	err := l.Load(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "order.created")
}

func TestLoadBrokenJSONSchemaFailsFast(t *testing.T) {
	l := schema.NewLoaderFromFS(fsWith(map[string]string{
		"order.created.json": `{"type": "not-a-type"}`,
	}), "schemas")
	err := l.Load(context.Background())
	require.Error(t, err)
}

func TestValidatePayloadOK(t *testing.T) {
	l := schema.NewLoaderFromFS(fsWith(map[string]string{
		"order.created.json": orderCreatedSchema,
	}), "schemas")
	require.NoError(t, l.Load(context.Background()))

	err := l.Validate(context.Background(), "order.created",
		json.RawMessage(`{"order_id":1,"customer_id":2,"amount":19.9}`))
	assert.NoError(t, err)
}

func TestValidatePayloadInvalid(t *testing.T) {
	l := schema.NewLoaderFromFS(fsWith(map[string]string{
		"order.created.json": orderCreatedSchema,
	}), "schemas")
	require.NoError(t, l.Load(context.Background()))

	err := l.Validate(context.Background(), "order.created",
		json.RawMessage(`{"order_id":"não é número","customer_id":2,"amount":19.9}`))
	assert.Error(t, err)
}

func TestValidateSchemaNotFound(t *testing.T) {
	l := schema.NewLoaderFromFS(fsWith(map[string]string{
		"order.created.json": orderCreatedSchema,
	}), "schemas")
	require.NoError(t, l.Load(context.Background()))

	err := l.Validate(context.Background(), "unknown.type",
		json.RawMessage(`{}`))
	require.Error(t, err)
	assert.ErrorIs(t, err, schema.ErrSchemaNotFound)
}
