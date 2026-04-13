package test

import (
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldApplySequenceBoundariesGivenSegmentDataWhenConsumeSegmentRuns(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			ctx := t.Context()
			storeID := uuid.New()
			space := "space-boundaries"
			segment := "segment-boundaries"

			statuses, err := enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, generateRange(0, 10)))
			require.NoError(t, err)
			require.NotEmpty(t, statuses)

			allEntries, err := enumerators.ToSlice(harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
				Space:   space,
				Segment: segment,
			}))
			require.NoError(t, err)
			require.Len(t, allEntries, 10)

			zeroMinEntries, err := enumerators.ToSlice(harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
				Space:       space,
				Segment:     segment,
				MinSequence: 0,
			}))
			require.NoError(t, err)
			assert.Len(t, zeroMinEntries, 10)

			singleEntries, err := enumerators.ToSlice(harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
				Space:       space,
				Segment:     segment,
				MinSequence: 3,
				MaxSequence: 3,
			}))
			require.NoError(t, err)
			require.Len(t, singleEntries, 1)
			assert.Equal(t, uint64(3), singleEntries[0].Sequence)

			rangeEntries, err := enumerators.ToSlice(harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
				Space:       space,
				Segment:     segment,
				MinSequence: 3,
				MaxSequence: 5,
			}))
			require.NoError(t, err)
			require.Len(t, rangeEntries, 3)
			assert.Equal(t, uint64(3), rangeEntries[0].Sequence)
			assert.Equal(t, uint64(5), rangeEntries[2].Sequence)

			beyondTailEntries, err := enumerators.ToSlice(harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
				Space:       space,
				Segment:     segment,
				MinSequence: 11,
			}))
			require.NoError(t, err)
			assert.Len(t, beyondTailEntries, 0)

			tailWindowEntries, err := enumerators.ToSlice(harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
				Space:       space,
				Segment:     segment,
				MinSequence: 9,
				MaxSequence: 15,
			}))
			require.NoError(t, err)
			require.Len(t, tailWindowEntries, 2)
			assert.Equal(t, uint64(9), tailWindowEntries[0].Sequence)
			assert.Equal(t, uint64(10), tailWindowEntries[1].Sequence)
		})
	}
}
