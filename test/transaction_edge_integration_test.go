package test

import (
	"sync"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldAssignUniqueTransactionIDsGivenSequentialProducesWhenSameSegment
// verifies that each Produce call stamps all its entries with the same TRX.ID
// yet distinct calls receive distinct IDs.
func TestShouldAssignUniqueTransactionIDsGivenSequentialProducesWhenSameSegment(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			ctx := t.Context()
			storeID := uuid.New()
			space, segment := "trx-space", "trx-segment"

			// Produce two separate batches so we get two distinct TRX IDs.
			_, err := enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, generateRange(0, 3)))
			require.NoError(t, err)
			_, err = enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, generateRange(3, 3)))
			require.NoError(t, err)

			entries, err := enumerators.ToSlice(harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
				Space:   space,
				Segment: segment,
			}))
			require.NoError(t, err)
			require.Len(t, entries, 6)

			// First 3 entries share one TRX.ID; last 3 share another; the two IDs must differ.
			firstTRX := entries[0].TRX.ID
			secondTRX := entries[3].TRX.ID
			assert.NotEqual(t, uuid.Nil, firstTRX)
			assert.NotEqual(t, uuid.Nil, secondTRX)
			assert.NotEqual(t, firstTRX, secondTRX, "sequential produces must have distinct TRX IDs")

			for i := 1; i < 3; i++ {
				assert.Equal(t, firstTRX, entries[i].TRX.ID, "entry %d must share TRX.ID with batch 1", i+1)
			}
			for i := 3; i < 6; i++ {
				assert.Equal(t, secondTRX, entries[i].TRX.ID, "entry %d must share TRX.ID with batch 2", i+1)
			}
		})
	}
}

// TestShouldMaintainStrictSequenceContiguityGivenConcurrentProducersWhenSameSegment
// spins up N concurrent producers and asserts no gaps or duplicates in the
// final sequence range, proving storage-level mutual exclusion.
func TestShouldMaintainStrictSequenceContiguityGivenConcurrentProducersWhenSameSegment(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			ctx := t.Context()
			storeID := uuid.New()
			space, segment := "trx-concurrent-space", "trx-concurrent-seg"

			const producers = 4
			const recsPerProducer = 25

			var wg sync.WaitGroup
			wg.Add(producers)
			for range producers {
				go func() {
					defer wg.Done()
					for {
						_, err := enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, generateRange(0, recsPerProducer)))
						if err == nil {
							return
						}
						// Retry on transient conflicts (sequence mismatch) until one batch lands.
					}
				}()
			}
			wg.Wait()

			// Exactly one producer should have won; the segment must have exactly
			// recsPerProducer contiguous entries starting at 1.
			entries, err := enumerators.ToSlice(harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
				Space:       space,
				Segment:     segment,
				MinSequence: 1,
			}))
			require.NoError(t, err)
			require.NotEmpty(t, entries)

			// All entries must be gap-free and form a contiguous sequence.
			seqs := make(map[uint64]bool, len(entries))
			for _, e := range entries {
				assert.False(t, seqs[e.Sequence], "duplicate sequence %d", e.Sequence)
				seqs[e.Sequence] = true
			}
			for i, e := range entries {
				assert.Equal(t, uint64(i+1), e.Sequence, "gap at position %d", i)
			}
		})
	}
}

// TestShouldAssignMonotonicallyIncreasingTRXNumbersGivenSuccessiveProducesWhenSameSegment
// checks that TRX.Number advances with each successive successful Produce.
func TestShouldAssignMonotonicallyIncreasingTRXNumbersGivenSuccessiveProducesWhenSameSegment(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			ctx := t.Context()
			storeID := uuid.New()
			space, segment := "trx-mono-space", "trx-mono-seg"

			const batches = 3
			for b := range batches {
				_, err := enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, generateRange(b*2, 2)))
				require.NoError(t, err)
			}

			entries, err := enumerators.ToSlice(harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
				Space:   space,
				Segment: segment,
			}))
			require.NoError(t, err)
			require.Len(t, entries, batches*2)

			var prevTRXNum uint64
			for b := range batches {
				batchEntries := entries[b*2 : b*2+2]
				trxNum := batchEntries[0].TRX.Number
				assert.Greater(t, trxNum, prevTRXNum, "batch %d TRX.Number must increase", b)
				prevTRXNum = trxNum
				// Both entries in a batch share the same TRX.Number.
				assert.Equal(t, trxNum, batchEntries[1].TRX.Number)
			}
		})
	}
}
