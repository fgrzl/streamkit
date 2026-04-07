package pebblekit

import (
	"context"
	"math"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/streamkit/internal/cache"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestPebbleStore(t *testing.T) *PebbleStore {
	t.Helper()

	store, err := NewPebbleStore(t.TempDir(), cache.NewExpiringCache(CacheTTL, CacheCleanupInterval))
	require.NoError(t, err)
	t.Cleanup(func() {
		store.Close()
	})
	return store
}

func TestShouldClampSpaceTimeBoundsToCurrentTimestamp(t *testing.T) {
	store := &PebbleStore{}

	tests := []struct {
		name        string
		current     int64
		min         int64
		max         int64
		expectedMin int64
		expectedMax int64
	}{
		{
			name:        "future min and unspecified max",
			current:     100,
			min:         200,
			max:         0,
			expectedMin: 100,
			expectedMax: 100,
		},
		{
			name:        "future max clamps to current",
			current:     100,
			min:         50,
			max:         200,
			expectedMin: 50,
			expectedMax: 100,
		},
		{
			name:        "bounded range preserved",
			current:     100,
			min:         10,
			max:         90,
			expectedMin: 10,
			expectedMax: 90,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bounds := store.calculateTimeBounds(tt.current, tt.min, tt.max)

			assert.Equal(t, tt.expectedMin, bounds.Min)
			assert.Equal(t, tt.expectedMax, bounds.Max)
		})
	}
}

func TestShouldUseOffsetAsLowerBoundWhenConsumingSpace(t *testing.T) {
	store := &PebbleStore{}
	offset := lexkey.Encode("custom-offset", 42)

	assert.Equal(t,
		lexkey.EncodeFirst(offset),
		store.getSpaceLowerBound("space-a", 123, offset),
	)
	assert.Equal(t,
		lexkey.EncodeFirst(api.DATA, api.SPACES, "space-a", int64(123)),
		store.getSpaceLowerBound("space-a", 123, nil),
	)
}

func TestShouldNormalizeSegmentBoundsWhenConsumingSegment(t *testing.T) {
	store := &PebbleStore{}

	tests := []struct {
		name           string
		args           *api.ConsumeSegment
		expectedMinSeq uint64
		expectedMaxSeq uint64
		expectedMinTS  int64
		expectedMaxTS  int64
	}{
		{
			name: "unbounded sequence and timestamp",
			args: &api.ConsumeSegment{
				MinSequence:  5,
				MinTimestamp: 200,
				MaxSequence:  0,
				MaxTimestamp: 0,
			},
			expectedMinSeq: 5,
			expectedMaxSeq: math.MaxUint64,
			expectedMinTS:  100,
			expectedMaxTS:  math.MaxInt64,
		},
		{
			name: "max sequence below min sequence clamps upward",
			args: &api.ConsumeSegment{
				MinSequence:  10,
				MinTimestamp: 20,
				MaxSequence:  3,
				MaxTimestamp: 150,
			},
			expectedMinSeq: 10,
			expectedMaxSeq: 10,
			expectedMinTS:  20,
			expectedMaxTS:  100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bounds := store.calculateSegmentBounds(100, tt.args)

			assert.Equal(t, tt.expectedMinSeq, bounds.MinSeq)
			assert.Equal(t, tt.expectedMaxSeq, bounds.MaxSeq)
			assert.Equal(t, tt.expectedMinTS, bounds.MinTS)
			assert.Equal(t, tt.expectedMaxTS, bounds.MaxTS)
		})
	}
}

func TestShouldBuildSegmentScanBoundsWithInclusiveMinimum(t *testing.T) {
	store := &PebbleStore{}

	lower, upper := store.getSegmentBounds("space-a", "segment-a", 10, 99)
	assert.Equal(t, lexkey.Encode(api.DATA, api.SEGMENTS, "space-a", "segment-a", uint64(10)), lower)
	assert.Equal(t, lexkey.EncodeLast(api.DATA, api.SEGMENTS, "space-a", "segment-a", uint64(99)), upper)

	lower, upper = store.getSegmentBounds("space-a", "segment-a", 0, 0)
	assert.Equal(t, lexkey.EncodeFirst(api.DATA, api.SEGMENTS, "space-a", "segment-a"), lower)
	assert.Equal(t, lexkey.EncodeLast(api.DATA, api.SEGMENTS, "space-a", "segment-a"), upper)
}

func TestShouldEncodeInventoryKeysConsistently(t *testing.T) {
	assert.EqualValues(t,
		lexkey.Encode(api.INVENTORY, api.SEGMENTS, "space-a", "segment-a"),
		encodeSegmentInventoryKey("space-a", "segment-a"),
	)
	assert.EqualValues(t,
		lexkey.Encode(api.INVENTORY, api.SPACES, "space-a"),
		encodeSpaceInventoryKey("space-a"),
	)
}

func TestShouldRejectInvalidProduceArguments(t *testing.T) {
	store := newTestPebbleStore(t)

	_, err := enumerators.ToSlice(store.Produce(context.Background(), nil, enumerators.Slice([]*api.Record{})))
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid produce args")

	_, err = enumerators.ToSlice(store.Produce(context.Background(), &api.Produce{Space: "", Segment: "segment-a"}, enumerators.Slice([]*api.Record{})))
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid produce args")
}

func TestShouldProducePeekAndEnumerateInventory(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	args := &api.Produce{Space: "space-a", Segment: "segment-a"}
	records := enumerators.Slice([]*api.Record{
		{Sequence: 1, Payload: []byte("first"), Metadata: map[string]string{"k": "v"}},
		{Sequence: 2, Payload: []byte("second"), Metadata: map[string]string{"k2": "v2"}},
	})

	statuses, err := enumerators.ToSlice(store.Produce(ctx, args, records))
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	assert.Equal(t, uint64(1), statuses[0].FirstSequence)
	assert.Equal(t, uint64(2), statuses[0].LastSequence)

	peeked, err := store.Peek(ctx, "space-a", "segment-a")
	require.NoError(t, err)
	require.NotNil(t, peeked)
	assert.Equal(t, uint64(2), peeked.Sequence)
	assert.Equal(t, []byte("second"), peeked.Payload)

	spaces, err := enumerators.ToSlice(store.GetSpaces(ctx))
	require.NoError(t, err)
	assert.Equal(t, []string{"space-a"}, spaces)

	segments, err := enumerators.ToSlice(store.GetSegments(ctx, "space-a"))
	require.NoError(t, err)
	assert.Equal(t, []string{"segment-a"}, segments)

	segmentEntries, err := enumerators.ToSlice(store.ConsumeSegment(ctx, &api.ConsumeSegment{Space: "space-a", Segment: "segment-a", MinSequence: 1}))
	require.NoError(t, err)
	require.Len(t, segmentEntries, 2)
	assert.Equal(t, uint64(1), segmentEntries[0].Sequence)
	assert.Equal(t, uint64(2), segmentEntries[1].Sequence)

	spaceEntries, err := enumerators.ToSlice(store.ConsumeSpace(ctx, &api.ConsumeSpace{Space: "space-a"}))
	require.NoError(t, err)
	require.Len(t, spaceEntries, 2)
	assert.Equal(t, "space-a", spaceEntries[0].Space)
	assert.Equal(t, "space-a", spaceEntries[1].Space)
}

func TestShouldReturnSequenceGapErrorWhenProduceSkipsSequence(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	args := &api.Produce{Space: "space-a", Segment: "segment-a"}

	_, err := enumerators.ToSlice(store.Produce(ctx, args, enumerators.Slice([]*api.Record{{Sequence: 1}, {Sequence: 2}})))
	require.NoError(t, err)

	_, err = enumerators.ToSlice(store.Produce(ctx, args, enumerators.Slice([]*api.Record{{Sequence: 4}})))
	require.Error(t, err)
	assert.ErrorContains(t, err, "sequence gap: expected 3, got 4")
}
