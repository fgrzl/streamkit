package test

import (
	"context"
	"testing"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestShouldStopConsumeSegmentGivenContextCanceledWhenStreamIsActive(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			storeID := uuid.New()
			space := "space-cancel"
			segment := "segment-cancel"
			const totalRecords = 300

			statuses, err := enumerators.ToSlice(harness.Client.Produce(t.Context(), storeID, space, segment, generateRange(0, totalRecords)))
			require.NoError(t, err)
			require.NotEmpty(t, statuses)

			consumeCtx, cancelConsume := context.WithCancel(t.Context())
			defer cancelConsume()

			enum := harness.Client.ConsumeSegment(consumeCtx, storeID, &client.ConsumeSegment{
				Space:       space,
				Segment:     segment,
				MinSequence: 1,
			})
			defer enum.Dispose()

			require.True(t, enum.MoveNext())
			_, err = enum.Current()
			require.NoError(t, err)

			cancelConsume()

			done := make(chan struct {
				count int
				err   error
			}, 1)

			go func() {
				count := 1
				for enum.MoveNext() {
					_, currentErr := enum.Current()
					if currentErr != nil {
						done <- struct {
							count int
							err   error
						}{count: count, err: currentErr}
						return
					}
					count++
					if count > totalRecords*2 {
						done <- struct {
							count int
							err   error
						}{count: count, err: context.DeadlineExceeded}
						return
					}
				}
				done <- struct {
					count int
					err   error
				}{count: count, err: enum.Err()}
			}()

			select {
			case result := <-done:
				require.LessOrEqual(t, result.count, totalRecords)
				if result.err != nil {
					require.ErrorIs(t, result.err, context.Canceled)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("consume stream did not terminate after context cancellation")
			}
		})
	}
}
