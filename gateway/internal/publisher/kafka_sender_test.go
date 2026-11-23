package publisher_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/victorvcruz/event-gateway/gateway/internal/publisher"
	"github.com/victorvcruz/event-gateway/shared/outbox"
)

func startKafka(t *testing.T) (brokers []string, teardown func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("tcc-poc"))
	require.NoError(t, err)

	brokers, err = container.Brokers(ctx)
	require.NoError(t, err)

	return brokers, func() { _ = container.Terminate(context.Background()) }
}

func createTopic(t *testing.T, brokers []string, topic string, partitions int32) {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer c.Close()

	admin := kadm.NewClient(c)
	_, err = admin.CreateTopics(context.Background(), partitions, 1, nil, topic)
	require.NoError(t, err)
}

func newConsumer(t *testing.T, brokers []string, topic string) *kgo.Client {
	t.Helper()
	c, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(fmt.Sprintf("test-%d", time.Now().UnixNano())),
		kgo.DisableAutoCommit(),
	)
	require.NoError(t, err)
	return c
}

func TestKafkaSenderProducesWithCorrelationKey(t *testing.T) {
	brokers, teardown := startKafka(t)
	defer teardown()

	createTopic(t, brokers, "order.created", 3)

	sender, err := publisher.NewKafkaSender(brokers)
	require.NoError(t, err)
	defer sender.Close()

	ctx := context.Background()
	row := outbox.Row{
		EventID:       uuid.New(),
		EventType:     "order.created",
		CorrelationID: "saga-abc",
		Payload:       json.RawMessage(`{"order_id":"42"}`),
	}
	require.NoError(t, sender.Send(ctx, row))

	consumer := newConsumer(t, brokers, row.EventType)
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	fetches := consumer.PollFetches(fetchCtx)
	require.False(t, fetches.IsClientClosed())

	var records []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) { records = append(records, r) })
	require.Len(t, records, 1)

	assert.Equal(t, "saga-abc", string(records[0].Key))
	assert.JSONEq(t, `{"order_id":"42"}`, string(records[0].Value))
	assert.Equal(t, "order.created", records[0].Topic)
}

func TestKafkaSenderSameCorrelationSamePartition(t *testing.T) {
	brokers, teardown := startKafka(t)
	defer teardown()

	createTopic(t, brokers, "order.created", 3)

	sender, err := publisher.NewKafkaSender(brokers)
	require.NoError(t, err)
	defer sender.Close()

	ctx := context.Background()
	cid := "saga-partition-check"
	for i := range 3 {
		row := outbox.Row{
			EventID:       uuid.New(),
			EventType:     "order.created",
			CorrelationID: cid,
			Payload:       json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)),
		}
		require.NoError(t, sender.Send(ctx, row))
	}

	consumer := newConsumer(t, brokers, "order.created")
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var partitions []int32
	for len(partitions) < 3 {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) { partitions = append(partitions, r.Partition) })
		if fetchCtx.Err() != nil {
			break
		}
	}
	require.Len(t, partitions, 3)

	for _, p := range partitions[1:] {
		assert.Equal(t, partitions[0], p, "same correlation_id must land on same partition")
	}
}
