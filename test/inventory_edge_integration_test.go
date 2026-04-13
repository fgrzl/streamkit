package test

import (
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnEmptyResultsGivenFreshStoreWhenQueryingInventory(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			ctx := t.Context()
			storeID := uuid.New()

			spaces, err := enumerators.ToSlice(harness.Client.GetSpaces(ctx, storeID))
			require.NoError(t, err)
			assert.Len(t, spaces, 0)

			segments, err := enumerators.ToSlice(harness.Client.GetSegments(ctx, storeID, "missing-space"))
			require.NoError(t, err)
			assert.Len(t, segments, 0)
		})
	}
}

func TestShouldReturnDefaultPeekAndEmptyConsumesGivenMissingSegmentWhenReadOperationsRun(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			ctx := t.Context()
			storeID := uuid.New()

			peek, err := harness.Client.Peek(ctx, storeID, "missing-space", "missing-segment")
			require.NoError(t, err)
			require.NotNil(t, peek)
			assert.Equal(t, "missing-space", peek.Space)
			assert.Equal(t, "missing-segment", peek.Segment)
			assert.Equal(t, uint64(0), peek.Sequence)

			segmentEntries, err := enumerators.ToSlice(harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
				Space:       "missing-space",
				Segment:     "missing-segment",
				MinSequence: 1,
			}))
			require.NoError(t, err)
			assert.Len(t, segmentEntries, 0)

			spaceEntries, err := enumerators.ToSlice(harness.Client.ConsumeSpace(ctx, storeID, &client.ConsumeSpace{
				Space: "missing-space",
			}))
			require.NoError(t, err)
			assert.Len(t, spaceEntries, 0)

			consumeEntries, err := enumerators.ToSlice(harness.Client.Consume(ctx, storeID, &client.Consume{
				Offsets: map[string]lexkey.LexKey{"missing-space": {}},
			}))
			require.NoError(t, err)
			assert.Len(t, consumeEntries, 0)
		})
	}
}
