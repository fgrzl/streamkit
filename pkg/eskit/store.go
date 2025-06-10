package eskit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/es"
	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit"
)

func NewStreamStore(client streamkit.Client) es.Store {
	return &streamStore{
		client: client,
	}
}

type streamStore struct {
	client streamkit.Client
}

func (s *streamStore) LoadEvents(ctx context.Context, entity es.Entity, minSequence uint64) ([]es.DomainEvent, error) {
	slog.Debug("Loading events", "space", entity.Area, "segment", entity.ID.String(), "minSequence", minSequence)

	args := &streamkit.ConsumeSegment{
		Space:       entity.Area,
		Segment:     entity.ID.String(),
		MinSequence: minSequence,
	}

	domainEvents := enumerators.Map(
		s.client.ConsumeSegment(ctx, entity.TenantID, args),
		func(entry *streamkit.Entry) (es.DomainEvent, error) {
			envelope := &polymorphic.Envelope{}
			if err := json.Unmarshal(entry.Payload, envelope); err != nil {
				slog.Error("Failed to unmarshal envelope", "error", err)
				return nil, err
			}
			domainEvent, ok := envelope.Content.(es.DomainEvent)
			if !ok {
				slog.Error("Invalid DomainEvent type", "actualSpace", fmt.Sprintf("%T", envelope.Content))
				return nil, fmt.Errorf("failed to cast to DomainEvent: %T", envelope.Content)
			}
			return domainEvent, nil
		})

	return enumerators.ToSlice(domainEvents)
}

func (s *streamStore) SaveEvents(ctx context.Context, entity es.Entity, events []es.DomainEvent, expectedSequence uint64) error {
	slog.Debug("Saving events", "space", entity.Area, "segment", entity.ID.String(), "expectedSequence", expectedSequence, "eventCount", len(events))

	space, segment := entity.Area, entity.ID.String()
	records := enumerators.Map(
		enumerators.Slice(events),
		func(event es.DomainEvent) (*streamkit.Record, error) {
			envelope := polymorphic.NewEnvelope(event)
			payload, err := json.Marshal(envelope)
			if err != nil {
				slog.Error("Failed to marshal event", "error", err)
				return nil, err
			}
			entry := &streamkit.Record{
				Sequence: event.GetSequence(),
				Payload:  payload,
			}
			return entry, nil
		})

	results := s.client.Produce(ctx, entity.TenantID, space, segment, records)
	if err := enumerators.Consume(results); err != nil {
		slog.Error("Failed to produce events", "error", err)
		return err
	}

	slog.Debug("Events saved successfully")
	return nil
}
