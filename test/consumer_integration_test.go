package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldConsumerOperations(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			storeID := uuid.New()
			ctx := t.Context()

			setupConsumerData(t, storeID, harness.Client)

			t.Run("should get spaces", func(t *testing.T) {
				enumerator := harness.Client.GetSpaces(ctx, storeID)
				spaces, err := enumerators.ToSlice(enumerator)

				require.NoError(t, err)
				assert.Len(t, spaces, 5)
				assert.Equal(t, "space0", spaces[0])
				assert.Equal(t, "space1", spaces[1])
				assert.Equal(t, "space2", spaces[2])
				assert.Equal(t, "space3", spaces[3])
				assert.Equal(t, "space4", spaces[4])
			})

			t.Run("should get segments", func(t *testing.T) {
				enumerator := harness.Client.GetSegments(ctx, storeID, "space0")
				segments, err := enumerators.ToSlice(enumerator)

				require.NoError(t, err)
				assert.Len(t, segments, 5)
				assert.Equal(t, "segment0", segments[0])
				assert.Equal(t, "segment1", segments[1])
				assert.Equal(t, "segment2", segments[2])
				assert.Equal(t, "segment3", segments[3])
				assert.Equal(t, "segment4", segments[4])
			})

			t.Run("should peek", func(t *testing.T) {
				peek, err := harness.Client.Peek(ctx, storeID, "space0", "segment0")

				require.NoError(t, err)
				assert.Equal(t, "space0", peek.Space)
				assert.Equal(t, "segment0", peek.Segment)
				assert.Equal(t, uint64(IntegrationSegmentCount), peek.Sequence)
			})

			t.Run("should consume segment", func(t *testing.T) {
				args := &client.ConsumeSegment{Space: "space0", Segment: "segment0"}
				results := harness.Client.ConsumeSegment(ctx, storeID, args)
				entries, err := enumerators.ToSlice(results)

				require.NoError(t, err)
				assert.Len(t, entries, IntegrationSegmentCount)
			})

			t.Run("should consume segment with inclusive min", func(t *testing.T) {
				args := &client.ConsumeSegment{Space: "space0", Segment: "segment0", MinSequence: 233}
				results := harness.Client.ConsumeSegment(ctx, storeID, args)
				entries, err := enumerators.ToSlice(results)

				require.NoError(t, err)
				expected := IntegrationSegmentCount - 233 + 1
				if expected < 0 {
					expected = 0
				}
				assert.Len(t, entries, expected)
			})

			t.Run("should consume space", func(t *testing.T) {
				args := &client.ConsumeSpace{Space: "space0"}
				expected := 5 * IntegrationSegmentCount

				var entries []*client.Entry
				var err error
				for attempt := 0; attempt < 10; attempt++ {
					results := harness.Client.ConsumeSpace(ctx, storeID, args)
					entries, err = enumerators.ToSlice(results)
					if err == nil && len(entries) == expected {
						break
					}
					t.Logf("attempt %d: ConsumeSpace(space0) returned %d entries (err=%v)", attempt+1, len(entries), err)
					time.Sleep(100 * time.Millisecond)
				}

				require.NoError(t, err)
				assert.Len(t, entries, expected, "ConsumeSpace(space0) should return %d entries after retries", expected)
			})

			t.Run("should consume interleaved spaces", func(t *testing.T) {
				ensureSpaceCounts := func() {
					var lastCounts [5]int
					for attempt := 0; attempt < 10; attempt++ {
						ok := true
						for i := 0; i < 5; i++ {
							space := fmt.Sprintf("space%d", i)
							enum := harness.Client.ConsumeSpace(ctx, storeID, &client.ConsumeSpace{Space: space})
							entries, err := enumerators.ToSlice(enum)
							lastCounts[i] = len(entries)
							if err != nil || len(entries) != 5*IntegrationSegmentCount {
								ok = false
								t.Logf("attempt %d: space %s has %d entries (err=%v)", attempt, space, len(entries), err)
							}
						}
						if ok {
							return
						}
						time.Sleep(100 * time.Millisecond)
					}
					t.Fatalf("setupConsumerData did not stabilize after retries, lastCounts=%v", lastCounts)
				}
				ensureSpaceCounts()

				args := &client.Consume{Offsets: map[string]lexkey.LexKey{
					"space0": {},
					"space1": {},
					"space2": {},
					"space3": {},
					"space4": {},
				}}

				var entries []*client.Entry
				var err error
				for attempt := 0; attempt < 10; attempt++ {
					results := harness.Client.Consume(ctx, storeID, args)
					entries, err = enumerators.ToSlice(results)
					if err == nil && len(entries) == 25*IntegrationSegmentCount {
						break
					}
					t.Logf("attempt %d: interleaved returned %d entries (err=%v)", attempt, len(entries), err)
					time.Sleep(100 * time.Millisecond)
				}

				require.NoError(t, err)
				assert.Len(t, entries, 25*IntegrationSegmentCount)
			})
		})
	}
}
