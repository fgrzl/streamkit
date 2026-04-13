package test

import (
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldPersistPayloadAndMetadataGivenPublishWhenConsumed(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			ctx := t.Context()
			storeID := uuid.New()
			space := "space-publish"
			segment := "segment-publish"

			payload := []byte("published-payload")
			metadata := map[string]string{"origin": "publish", "type": "edge"}

			err := harness.Client.Publish(ctx, storeID, space, segment, payload, metadata)
			require.NoError(t, err)

			entries, err := enumerators.ToSlice(harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
				Space:       space,
				Segment:     segment,
				MinSequence: 1,
			}))
			require.NoError(t, err)
			require.Len(t, entries, 1)
			assert.Equal(t, payload, entries[0].Payload)
			assert.Equal(t, metadata, entries[0].Metadata)
			assert.Equal(t, uint64(1), entries[0].Sequence)

			peek, err := harness.Client.Peek(ctx, storeID, space, segment)
			require.NoError(t, err)
			require.NotNil(t, peek)
			assert.Equal(t, uint64(1), peek.Sequence)
		})
	}
}
