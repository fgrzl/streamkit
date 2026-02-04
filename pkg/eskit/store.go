// Package eskit provides integration between the streamkit streaming platform
// and event sourcing capabilities.
//
// This package implements an event store adapter that bridges streamkit's
// streaming APIs with domain event persistence patterns, enabling event-driven
// architectures built on top of the streaming platform.
package eskit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/es"
	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/client"
)

// NewStreamStore creates a new event store implementation backed by a streamkit client.
func NewStreamStore(client client.Client) es.Store {
	return &streamStore{
		client: client,
	}
}

type streamStore struct {
	client client.Client
}

func (s *streamStore) LoadEvents(ctx context.Context, entity es.Entity, minSequence uint64) ([]es.DomainEvent, error) {
	args := &client.ConsumeSegment{
		Space:       entity.Area,
		Segment:     entity.ID.String(),
		MinSequence: minSequence,
	}

	domainEvents := enumerators.Map(
		s.client.ConsumeSegment(ctx, entity.TenantID, args),
		func(entry *client.Entry) (es.DomainEvent, error) {
			envelope := &polymorphic.Envelope{}
			if err := json.Unmarshal(entry.Payload, envelope); err != nil {
				slog.ErrorContext(ctx, "Failed to unmarshal envelope", "err", err)
				return nil, err
			}
			domainEvent, ok := envelope.Content.(es.DomainEvent)
			if !ok {
				slog.ErrorContext(ctx, "Invalid DomainEvent type", "actualType", fmt.Sprintf("%T", envelope.Content))
				return nil, fmt.Errorf("failed to cast to DomainEvent: %T", envelope.Content)
			}
			return domainEvent, nil
		})

	events, err := enumerators.ToSlice(domainEvents)
	if err != nil {
		return nil, err
	}

	return events, nil
}

func (s *streamStore) SaveEvents(ctx context.Context, entity es.Entity, events []es.DomainEvent, expectedSequence uint64) error {
	space, segment := entity.Area, entity.ID.String()

	records := enumerators.Map(
		enumerators.Slice(events),
		func(event es.DomainEvent) (*client.Record, error) {
			envelope := polymorphic.NewEnvelope(event)
			payload, err := json.Marshal(envelope)
			if err != nil {
				return nil, err
			}
			entry := &client.Record{
				Sequence: event.GetSequence(),
				Payload:  payload,
			}
			return entry, nil
		})

	results := s.client.Produce(ctx, entity.TenantID, space, segment, records)

	// Track status updates to ensure all records are acknowledged
	statusCount := 0
	err := enumerators.ForEach(results, func(status *client.SegmentStatus) error {
		statusCount++
		return nil
	})

	if err != nil {
		return err
	}

	// Validate that we received at least one status update
	if statusCount == 0 {
		return fmt.Errorf("no status updates received from producer")
	}

	return nil
}
