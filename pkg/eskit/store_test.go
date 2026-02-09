package eskit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/es"
	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/messaging"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDomainEvent implements the minimal es.DomainEvent used by the store.
type fakeDomainEvent struct {
	es.DomainEventBase
	seq uint64
}

// satisfy the minimal es.DomainEvent used by the store
func (f *fakeDomainEvent) GetSequence() uint64 {
	if f.Metadata.Sequence != 0 {
		return f.Metadata.Sequence
	}
	return f.seq
}
func (f *fakeDomainEvent) GetAggregateID() uuid.UUID { return uuid.Nil }

// satisfy polymorphic.Polymorphic
func (f *fakeDomainEvent) GetDiscriminator() string { return "fakeDomainEvent" }

// satisfy additional es.DomainEvent requirements
func (f *fakeDomainEvent) GetArea() string             { return "sp" }
func (f *fakeDomainEvent) GetCausationID() uuid.UUID   { return uuid.Nil }
func (f *fakeDomainEvent) GetCorrelationID() uuid.UUID { return uuid.Nil }
func (f *fakeDomainEvent) GetEntity() es.Entity {
	return es.Entity{Area: "sp", ID: uuid.Nil, TenantID: uuid.Nil}
}
func (f *fakeDomainEvent) GetEventID() uuid.UUID         { return uuid.Nil }
func (f *fakeDomainEvent) GetMetadata() es.EventMetadata { return es.EventMetadata{} }
func (f *fakeDomainEvent) GetSpaces() []string           { return []string{} }
func (f *fakeDomainEvent) GetTenantID() uuid.UUID        { return uuid.Nil }
func (f *fakeDomainEvent) GetTimestamp() int64           { return 0 }
func (f *fakeDomainEvent) SetMetadata(metadata es.EventMetadata) {
	f.Metadata = metadata
	if metadata.Sequence != 0 {
		f.seq = metadata.Sequence
	}
}
func (f *fakeDomainEvent) GetRoute() messaging.Route { return messaging.NewGlobalRoute("es", "fake") }

// MarshalJSON ensures the embedded metadata contains the sequence
// so that polymorphic.NewEnvelope can marshal/unmarshal the event
// and preserve sequence information during round-trip.
func (f *fakeDomainEvent) MarshalJSON() ([]byte, error) {
	// ensure metadata sequence is set from seq if present
	if f.Metadata.Sequence == 0 && f.seq != 0 {
		f.Metadata.Sequence = f.seq
	}
	// Only marshal the metadata (DomainEventBase contains the metadata field)
	type payload struct {
		Metadata es.EventMetadata `json:"metadata"`
	}
	return json.Marshal(payload{Metadata: f.Metadata})
}

// fakeClient implements the subset of streamkit.Client used by streamStore.
type fakeClient struct {
	consume  enumerators.Enumerator[*client.Entry]
	produced []*client.Record
	// Produce can be overridden in tests to customize behavior
	ProduceFunc func(ctx context.Context, storeID uuid.UUID, space, segment string, entries enumerators.Enumerator[*client.Record]) enumerators.Enumerator[*client.SegmentStatus]
}

func (f *fakeClient) ConsumeSegment(ctx context.Context, storeID uuid.UUID, args *client.ConsumeSegment) enumerators.Enumerator[*client.Entry] {
	return f.consume
}

func (f *fakeClient) Produce(ctx context.Context, storeID uuid.UUID, space, segment string, entries enumerators.Enumerator[*client.Record]) enumerators.Enumerator[*client.SegmentStatus] {
	if f.ProduceFunc != nil {
		return f.ProduceFunc(ctx, storeID, space, segment, entries)
	}

	// Default implementation: consume records and capture them
	for entries.MoveNext() {
		r, err := entries.Current()
		if err != nil {
			break
		}
		f.produced = append(f.produced, r)
	}
	return enumerators.Slice([]*client.SegmentStatus{{}})
}

// other client methods not used in these tests
func (f *fakeClient) GetSpaces(ctx context.Context, storeID uuid.UUID) enumerators.Enumerator[string] {
	return enumerators.Slice([]string{})
}
func (f *fakeClient) GetSegments(ctx context.Context, storeID uuid.UUID, space string) enumerators.Enumerator[string] {
	return enumerators.Slice([]string{})
}
func (f *fakeClient) Consume(ctx context.Context, storeID uuid.UUID, args *client.Consume) enumerators.Enumerator[*client.Entry] {
	return enumerators.Slice([]*client.Entry{})
}
func (f *fakeClient) ConsumeSpace(ctx context.Context, storeID uuid.UUID, args *client.ConsumeSpace) enumerators.Enumerator[*client.Entry] {
	return enumerators.Slice([]*client.Entry{})
}
func (f *fakeClient) Peek(ctx context.Context, storeID uuid.UUID, space, segment string) (*client.Entry, error) {
	return &client.Entry{}, nil
}
func (f *fakeClient) Publish(ctx context.Context, storeID uuid.UUID, space, segment string, payload []byte, metadata map[string]string) error {
	return nil
}
func (f *fakeClient) SubscribeToSpace(ctx context.Context, storeID uuid.UUID, space string, handler func(*client.SegmentStatus)) (api.Subscription, error) {
	return nil, nil
}
func (f *fakeClient) SubscribeToSegment(ctx context.Context, storeID uuid.UUID, space, segment string, handler func(*client.SegmentStatus)) (api.Subscription, error) {
	return nil, nil
}
func (f *fakeClient) GetSubscriptionStatus(id string) *client.SubscriptionStatus {
	return &client.SubscriptionStatus{}
}
func (f *fakeClient) Close() error {
	return nil
}

func TestLoadEventsShouldUnmarshalDomainEvents(t *testing.T) {
	// Arrange
	polymorphic.RegisterType[fakeDomainEvent]()
	evt := &fakeDomainEvent{seq: 5}
	env := polymorphic.NewEnvelope(evt)
	payload, err := json.Marshal(env)
	require.NoError(t, err)

	entry := &client.Entry{Payload: payload}
	fc := &fakeClient{consume: enumerators.Slice([]*client.Entry{entry})}
	s := &streamStore{client: fc}

	// Act
	got, err := s.LoadEvents(context.Background(), es.Entity{Area: "sp", ID: uuid.New(), TenantID: uuid.New()}, 0)

	// Assert
	require.NoError(t, err)
	require.Len(t, got, 1)
	// type assertion to our fakeDomainEvent
	de, ok := got[0].(interface{ GetSequence() uint64 })
	require.True(t, ok)
	assert.Equal(t, uint64(5), de.GetSequence())
}

func TestSaveEventsShouldProduceMarshaledRecords(t *testing.T) {
	// Arrange
	polymorphic.RegisterType[fakeDomainEvent]()
	events := []es.DomainEvent{&fakeDomainEvent{seq: 7}, &fakeDomainEvent{seq: 8}}
	fc := &fakeClient{}
	s := &streamStore{client: fc}

	// Act
	// expectedSequence of 6 indicates last known sequence for the aggregate was 6
	err := s.SaveEvents(context.Background(), es.Entity{Area: "sp", ID: uuid.New(), TenantID: uuid.New()}, events, 6)

	// Assert
	require.NoError(t, err)
	require.Len(t, fc.produced, 2)
	assert.Equal(t, uint64(7), fc.produced[0].Sequence)
	assert.Equal(t, uint64(8), fc.produced[1].Sequence)
}

func TestSaveEventsShouldReturnErrorWhenProducerFailsWithSequenceMismatch(t *testing.T) {
	// Arrange
	sequenceMismatchErr := errors.New("sequence mismatch")
	polymorphic.RegisterType[fakeDomainEvent]()
	events := []es.DomainEvent{&fakeDomainEvent{seq: 1}, &fakeDomainEvent{seq: 2}}

	fc := &fakeClient{}
	// Override Produce to return error after consuming records
	fc.ProduceFunc = func(ctx context.Context, storeID uuid.UUID, space, segment string, entries enumerators.Enumerator[*client.Record]) enumerators.Enumerator[*client.SegmentStatus] {
		enumerators.Consume(entries)
		return enumerators.Error[*client.SegmentStatus](sequenceMismatchErr)
	}

	s := &streamStore{client: fc}

	// Act
	err := s.SaveEvents(context.Background(), es.Entity{Area: "teams", ID: uuid.New(), TenantID: uuid.New()}, events, 0)

	// Assert
	assert.Error(t, err)
	assert.ErrorIs(t, err, sequenceMismatchErr)
}

func TestSaveEventsValidatesExpectedSequence(t *testing.T) {
	polymorphic.RegisterType[fakeDomainEvent]()
	fc := &fakeClient{}
	s := &streamStore{client: fc}
	// first event sequence is 2 but expectedSequence is 0 -> expect error
	events := []es.DomainEvent{&fakeDomainEvent{seq: 2}}
	err := s.SaveEvents(context.Background(), es.Entity{Area: "teams", ID: uuid.New(), TenantID: uuid.New()}, events, 0)
	require.Error(t, err)
}

func TestSaveEventsValidatesContiguousSequences(t *testing.T) {
	polymorphic.RegisterType[fakeDomainEvent]()
	fc := &fakeClient{}
	s := &streamStore{client: fc}
	// non-contiguous sequences (1,3)
	events := []es.DomainEvent{&fakeDomainEvent{seq: 1}, &fakeDomainEvent{seq: 3}}
	err := s.SaveEvents(context.Background(), es.Entity{Area: "teams", ID: uuid.New(), TenantID: uuid.New()}, events, 0)
	require.Error(t, err)
}

func TestSaveEventsShouldReturnErrorWhenNoStatusUpdatesReceived(t *testing.T) {
	// Arrange
	polymorphic.RegisterType[fakeDomainEvent]()
	events := []es.DomainEvent{&fakeDomainEvent{seq: 1}}

	fc := &fakeClient{}
	// Override Produce to return empty enumerator (no status updates)
	fc.ProduceFunc = func(ctx context.Context, storeID uuid.UUID, space, segment string, entries enumerators.Enumerator[*client.Record]) enumerators.Enumerator[*client.SegmentStatus] {
		enumerators.Consume(entries)
		return enumerators.Empty[*client.SegmentStatus]()
	}

	s := &streamStore{client: fc}

	// Act
	err := s.SaveEvents(context.Background(), es.Entity{Area: "teams", ID: uuid.New(), TenantID: uuid.New()}, events, 0)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no status updates received")
}
