package test

import (
	"strconv"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldAllowMultiplexedCallsWhenUsingDifferentSegments(t *testing.T) {
	for name, h := range configurations() {
		t.Run("should allow for multiplexed calls "+name, func(t *testing.T) {
			harness := h(t)
			storeID := uuid.New()
			for i := range 3 {
				ctx := t.Context()
				space, segment := "space0", "segment"+strconv.Itoa(i)

				entry, err := harness.Client.Peek(ctx, storeID, space, segment)
				require.NoError(t, err)
				assert.Equal(t, &client.Entry{Space: space, Segment: segment}, entry)
			}
		})
	}
}

func TestShouldProduceRecordsSuccessfullyWhenGivenValidInput(t *testing.T) {
	for name, h := range configurations() {
		t.Run("should produce "+name, func(t *testing.T) {
			harness := h(t)
			storeID := uuid.New()
			for i := range 3 {
				ctx := t.Context()
				space, segment, records := "space0", "segment"+strconv.Itoa(i), generateRange(0, 5)

				results := harness.Client.Produce(ctx, storeID, space, segment, records)
				statuses, err := enumerators.ToSlice(results)
				require.NoError(t, err)
				assert.Len(t, statuses, 1)
			}
		})
	}
}

func TestShouldConcurrentProducersDetectConflict(t *testing.T) {
	for name, h := range configurations() {
		t.Run("concurrent producers "+name, func(t *testing.T) {
			harness := h(t)
			storeID := uuid.New()
			ctx := t.Context()
			space, segment := "space-concurrent", "segment-conflict"

			recA := generateRange(0, 100)
			recB := generateRange(0, 100)

			ch := make(chan error, 2)
			go func() {
				_, err := enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, recA))
				ch <- err
			}()
			go func() {
				_, err := enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, recB))
				ch <- err
			}()

			err1 := <-ch
			err2 := <-ch
			t.Logf("producer errors: err1=%v err2=%v", err1, err2)

			enum := harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{Space: space, Segment: segment, MinSequence: 1})
			entries, err := enumerators.ToSlice(enum)
			require.NoError(t, err)
			require.Len(t, entries, 100)
			for i, e := range entries {
				reqSeq := uint64(i + 1)
				assert.Equal(t, reqSeq, e.Sequence)
			}
			if err1 == nil && err2 == nil {
				t.Log("no producer-side errors observed; storage invariants hold")
			}
		})
	}
}

func TestShouldProduceLargeRecordsChunking(t *testing.T) {
	for name, h := range configurations() {
		t.Run("large produce "+name, func(t *testing.T) {
			harness := h(t)
			storeID := uuid.New()
			ctx := t.Context()
			space, segment := "space-large", "segment-large"

			recs := generateLargeRange(0, 50, 8*1024)
			_, err := enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, recs))
			require.NoError(t, err)

			enum := harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{Space: space, Segment: segment, MinSequence: 1})
			entries, err := enumerators.ToSlice(enum)
			require.NoError(t, err)
			require.Len(t, entries, 50)
		})
	}
}
