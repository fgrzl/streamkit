package test

import (
	"sync"
	"testing"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldDeliverSegmentUpdatesGivenSubscriptionWhenRecordsAreProduced(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			ctx := t.Context()
			storeID := uuid.New()
			space := "space-sub-segment"
			segment := "segment-a"

			updates := make(chan client.SegmentStatus, 32)
			var seenMu sync.Mutex
			seenCount := 0

			sub, err := harness.Client.SubscribeToSegment(ctx, storeID, space, segment, func(status *client.SegmentStatus) {
				if status == nil || status.Heartbeat {
					return
				}
				seenMu.Lock()
				seenCount++
				seenMu.Unlock()
				select {
				case updates <- *status:
				default:
				}
			})
			require.NoError(t, err)
			t.Cleanup(func() { sub.Unsubscribe() })

			require.Eventually(t, func() bool {
				status := harness.Client.GetSubscriptionStatus(sub.ID())
				return status != nil && status.Status == "active"
			}, 3*time.Second, 25*time.Millisecond)

			statuses, err := enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, generateRange(0, 3)))
			require.NoError(t, err)
			require.NotEmpty(t, statuses)

			require.Eventually(t, func() bool {
				for {
					select {
					case st := <-updates:
						if st.Space == space && st.Segment == segment && st.LastSequence >= 3 {
							return true
						}
					default:
						return false
					}
				}
			}, 10*time.Second, 25*time.Millisecond)

			sub.Unsubscribe()

			seenMu.Lock()
			countBefore := seenCount
			seenMu.Unlock()

			statuses, err = enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, generateRange(3, 1)))
			require.NoError(t, err)
			require.NotEmpty(t, statuses)

			assert.Never(t, func() bool {
				seenMu.Lock()
				defer seenMu.Unlock()
				return seenCount > countBefore
			}, 300*time.Millisecond, 25*time.Millisecond)
		})
	}
}

func TestShouldDeliverSpaceUpdatesGivenWildcardSubscriptionWhenMultipleSegmentsChange(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			ctx := t.Context()
			storeID := uuid.New()
			space := "space-sub-space"

			updates := make(chan client.SegmentStatus, 64)
			sub, err := harness.Client.SubscribeToSpace(ctx, storeID, space, func(status *client.SegmentStatus) {
				if status == nil || status.Heartbeat {
					return
				}
				select {
				case updates <- *status:
				default:
				}
			})
			require.NoError(t, err)
			defer sub.Unsubscribe()

			require.Eventually(t, func() bool {
				status := harness.Client.GetSubscriptionStatus(sub.ID())
				return status != nil && status.Status == "active"
			}, 3*time.Second, 25*time.Millisecond)

			_, err = enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, "segment-a", generateRange(0, 2)))
			require.NoError(t, err)
			_, err = enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, "segment-b", generateRange(0, 2)))
			require.NoError(t, err)

			seen := map[string]uint64{}
			require.Eventually(t, func() bool {
				for {
					select {
					case st := <-updates:
						if st.Space != space {
							continue
						}
						if st.LastSequence > seen[st.Segment] {
							seen[st.Segment] = st.LastSequence
						}
					default:
						return seen["segment-a"] >= 2 && seen["segment-b"] >= 2
					}
				}
			}, 10*time.Second, 25*time.Millisecond)
		})
	}
}
