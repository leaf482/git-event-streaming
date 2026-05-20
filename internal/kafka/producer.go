package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/github-pulse/git-event-streaming/internal/events"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic, clientID string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: false,
			BatchTimeout:           100 * time.Millisecond,
			Transport: &kafka.Transport{
				ClientID: clientID,
			},
		},
	}
}

func (p *Producer) PublishEvent(ctx context.Context, event events.NormalizedEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal normalized event: %w", err)
	}

	message := kafka.Message{
		Key:   []byte(event.EventID),
		Value: payload,
		Time:  event.IngestedAt,
		Headers: []kafka.Header{
			{Key: "schema_version", Value: []byte(event.SchemaVersion)},
			{Key: "event_type", Value: []byte(event.EventType)},
			{Key: "source", Value: []byte(event.Source)},
		},
	}

	if err := p.writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("publish normalized event event_id=%s: %w", event.EventID, err)
	}

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
