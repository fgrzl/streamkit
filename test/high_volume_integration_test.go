package test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func highVolumeRecordsPerSegment(configName string) int {
	if testing.Short() {
		return 2500
	}

	switch configName {
	case "azure":
		return 2500
	case "pebble":
		return 3000
	default:
		return 4000
	}
}

func TestShouldHighVolumeReadWriteStream(t *testing.T) {
	for name, h := range configurations() {
		t.Run("high volume read/write "+name, func(t *testing.T) {
			harness := h(t)
			ctx := t.Context()
			storeID := uuid.New()

			const space = "space-high-volume"
			segments := []string{"segment-0", "segment-1", "segment-2", "segment-3"}
			recordsPerSegment := highVolumeRecordsPerSegment(name)
			batchSize := 250

			var wg sync.WaitGroup
			errCh := make(chan error, len(segments))

			for _, segment := range segments {
				segment := segment
				wg.Add(1)
				go func() {
					defer wg.Done()

					for start := 0; start < recordsPerSegment; start += batchSize {
						count := batchSize
						if remaining := recordsPerSegment - start; remaining < count {
							count = remaining
						}

						records := enumerators.Range(start, count, func(i int) *client.Record {
							sequence := uint64(i + 1)
							return &client.Record{
								Sequence: sequence,
								Payload:  []byte(fmt.Sprintf("high-volume-%s-%s-%d", space, segment, sequence)),
							}
						})

						statuses, err := enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, records))
						if err != nil {
							errCh <- fmt.Errorf("produce failed for %s at start=%d: %w", segment, start, err)
							return
						}
						if len(statuses) == 0 {
							errCh <- fmt.Errorf("produce returned no statuses for %s at start=%d", segment, start)
							return
						}
					}
				}()
			}

			wg.Wait()
			close(errCh)
			for err := range errCh {
				require.NoError(t, err)
			}

			totalExpected := len(segments) * recordsPerSegment
			require.GreaterOrEqual(t, totalExpected, 10000)
			entries, err := enumerators.ToSlice(harness.Client.Consume(ctx, storeID, &client.Consume{
				Offsets: map[string]lexkey.LexKey{space: {}},
			}))
			require.NoError(t, err)
			require.Len(t, entries, totalExpected)

			seenBySegment := make(map[string]map[uint64]struct{}, len(segments))
			for _, segment := range segments {
				seenBySegment[segment] = make(map[uint64]struct{}, recordsPerSegment)
			}

			for _, entry := range entries {
				require.Equal(t, space, entry.Space)
				segmentSeen, ok := seenBySegment[entry.Segment]
				require.True(t, ok, "unexpected segment %q", entry.Segment)
				_, duplicate := segmentSeen[entry.Sequence]
				require.False(t, duplicate, "duplicate sequence=%d segment=%s", entry.Sequence, entry.Segment)
				segmentSeen[entry.Sequence] = struct{}{}
			}

			for _, segment := range segments {
				segmentSeen := seenBySegment[segment]
				require.Len(t, segmentSeen, recordsPerSegment, "unexpected record count for segment %s", segment)
				for seq := 1; seq <= recordsPerSegment; seq++ {
					_, ok := segmentSeen[uint64(seq)]
					require.True(t, ok, "missing sequence=%d segment=%s", seq, segment)
				}

				peek, err := harness.Client.Peek(ctx, storeID, space, segment)
				require.NoError(t, err)
				assert.Equal(t, uint64(recordsPerSegment), peek.Sequence)
			}
		})
	}
}
