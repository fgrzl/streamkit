package test

import (
	"fmt"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type TestHarness struct {
	storage.Store
	streamkit.Client
}

func setupConsumerData(t *testing.T, storeID uuid.UUID, client streamkit.Client) {
	ctx := t.Context()

	for i := range 5 {
		for j := range 5 {
			space, segment, records := fmt.Sprintf("space%d", i), fmt.Sprintf("segment%d", j), generateRange(0, 253)
			results := client.Produce(ctx, storeID, space, segment, records)
			err := enumerators.Consume(results)
			require.NoError(t, err)
		}
	}
}

func generateRange(seed, count int) enumerators.Enumerator[*streamkit.Record] {
	return enumerators.Range(seed, count, func(i int) *streamkit.Record {
		return &streamkit.Record{
			Sequence: uint64(i + 1),
			Payload:  []byte(fmt.Sprintf("test data %d", i+1)),
		}
	})
}
