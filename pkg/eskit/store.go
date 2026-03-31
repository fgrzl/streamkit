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
				slog.ErrorContext(ctx, "eskit: failed to unmarshal event envelope",
					slog.String("store_id", entity.TenantID.String()),
					slog.String("space", entity.Area),
					slog.String("segment", entity.ID.String()),
					slog.Uint64("entry_sequence", entry.Sequence),
					slog.Int("payload_bytes", len(entry.Payload)),
					"err", err)
				return nil, err
			}
			domainEvent, ok := envelope.Content.(es.DomainEvent)
			if !ok {
				slog.ErrorContext(ctx, "eskit: invalid domain event type",
					slog.String("store_id", entity.TenantID.String()),
					slog.String("space", entity.Area),
					slog.String("segment", entity.ID.String()),
					slog.Uint64("entry_sequence", entry.Sequence),
					slog.String("actual_type", fmt.Sprintf("%T", envelope.Content)))
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
	// ExpectedSequence contract:
	// - The caller must provide the last known sequence for the aggregate (expectedSequence).
	// - The first event's sequence MUST equal expectedSequence + 1.
	// - Events must be strictly sequential (each next event seq == prev+1).
	// Partial appends and retries are possible due to the underlying at-least-once fanout.
	// Consumers should rely on sequence continuity for idempotency and correctness.
	if len(events) == 0 {
		return nil
	}

	// Validate expected sequence
	firstSeq := events[0].GetSequence()
	if firstSeq != expectedSequence+1 {
		return fmt.Errorf("expected sequence mismatch: expected %d, got %d", expectedSequence+1, firstSeq)
	}

	// Validate contiguous sequences
	prev := firstSeq
	for i := 1; i < len(events); i++ {
		seq := events[i].GetSequence()
		if seq != prev+1 {
			return fmt.Errorf("non-contiguous sequence at index %d: expected %d, got %d", i, prev+1, seq)
		}
		prev = seq
	}

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
		slog.ErrorContext(ctx, "eskit: failed to save events",
			slog.String("store_id", entity.TenantID.String()),
			slog.String("space", space),
			slog.String("segment", segment),
			slog.Int("event_count", len(events)),
			slog.Uint64("expected_sequence", expectedSequence),
			slog.Int("status_count", statusCount),
			"err", err)
		return err
	}

	// Validate that we received at least one status update
	if statusCount == 0 {
		slog.ErrorContext(ctx, "eskit: producer returned no status updates",
			slog.String("store_id", entity.TenantID.String()),
			slog.String("space", space),
			slog.String("segment", segment),
			slog.Int("event_count", len(events)),
			slog.Uint64("expected_sequence", expectedSequence))
		return fmt.Errorf("no status updates received from producer")
	}

	return nil
}
