package test

import (
	"runtime"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	streams "github.com/fgrzl/streamkit"

	"github.com/stretchr/testify/assert"
)

func TestProduce(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should produce "+name, func(t *testing.T) {

			// Arrange
			ctx := t.Context()
			space, segment, records := "space0", "segment0", generateRange(0, 5)

			// Act
			results := h.Client.Produce(ctx, space, segment, records)
			statuses, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, statuses, 1)
		})
	}
}

func TestGetSpaces(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should get spaces "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, h.Client)

			// Act
			enumerator := h.Client.GetSpaces(ctx)
			spaces, err := enumerators.ToSlice(enumerator)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, spaces, 5)
			assert.Equal(t, "space0", spaces[0])
			assert.Equal(t, "space1", spaces[1])
			assert.Equal(t, "space2", spaces[2])
			assert.Equal(t, "space3", spaces[3])
			assert.Equal(t, "space4", spaces[4])
		})
	}
}

func TestGetSegments(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should get segments "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, h.Client)

			// Act
			enumerator := h.Client.GetSegments(ctx, "space0")
			segments, err := enumerators.ToSlice(enumerator)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, segments, 5)
			assert.Equal(t, "segment0", segments[0])
			assert.Equal(t, "segment1", segments[1])
			assert.Equal(t, "segment2", segments[2])
			assert.Equal(t, "segment3", segments[3])
			assert.Equal(t, "segment4", segments[4])
		})
	}
}

func TestPeek(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should peek "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, h.Client)

			// Act
			peek, err := h.Client.Peek(ctx, "space0", "segment0")

			// Assert
			assert.NoError(t, err)
			assert.Equal(t, "space0", peek.Space)
			assert.Equal(t, "segment0", peek.Segment)
			assert.Equal(t, uint64(253), peek.Sequence)
		})
	}
}

func TestConsumeSegment(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should consume segment "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, h.Client)

			args := &streams.ConsumeSegment{
				Space:   "space0",
				Segment: "segment0",
			}

			// Act
			results := h.Client.ConsumeSegment(ctx, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, entries, 253)
		})
	}
}

func TestConsumeSpace(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should consume space "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, h.Client)

			args := &streams.ConsumeSpace{
				Space: "space0",
			}

			// Act
			results := h.Client.ConsumeSpace(ctx, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, entries, 1_265)
		})
	}
}

func TestConsume(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should consume interleaved spaces "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()

			setupConsumerData(t, h.Client)
			runtime.Gosched()

			args := &streams.Consume{
				Offsets: map[string]lexkey.LexKey{
					"space0": {},
					"space1": {},
					"space2": {},
					"space3": {},
					"space4": {},
				},
			}

			// Act
			results := h.Client.Consume(ctx, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, entries, 6_325)
		})
	}
}
