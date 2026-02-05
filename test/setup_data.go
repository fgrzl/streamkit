package test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type TestHarness struct {
	storage.Store
	client.Client
}

func setupConsumerData(t *testing.T, storeID uuid.UUID, c client.Client) {
	ctx := t.Context()

	for i := range 5 {
		for j := range 5 {
			space, segment, records := fmt.Sprintf("space%d", i), fmt.Sprintf("segment%d", j), generateRange(0, 253)
			results := c.Produce(ctx, storeID, space, segment, records)
			statuses, err := enumerators.ToSlice(results)
			require.NoError(t, err)
			require.NotEmpty(t, statuses, "expected at least one status from Produce")
		}
	}
}

func generateRange(seed, count int) enumerators.Enumerator[*client.Record] {
	return enumerators.Range(seed, count, func(i int) *client.Record {
		return &client.Record{
			Sequence: uint64(i + 1),
			Payload:  []byte(fmt.Sprintf("test data %d", i+1)),
		}
	})
}

func generateLargeRange(seed, count, size int) enumerators.Enumerator[*client.Record] {
	return enumerators.Range(seed, count, func(i int) *client.Record {
		return &client.Record{
			Sequence: uint64(i + 1),
			Payload:  bytes.Repeat([]byte("x"), size),
		}
	})
}
