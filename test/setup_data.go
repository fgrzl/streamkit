package test

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// IntegrationSegmentCount determines how many records are produced per segment during
// integration setup. It defaults to 253 (real size) but can be reduced by setting
// STREAMKIT_TEST_SMALL or STREAMKIT_TEST_RECORDS environment variables for faster
// local feedback.
var IntegrationSegmentCount = 253

type TestHarness struct {
	storage.Store
	client.Client
}

func setupConsumerData(t *testing.T, storeID uuid.UUID, c client.Client) {
	ctx := t.Context()

	// Determine per-segment count from env or test flags to speed up local runs
	count := IntegrationSegmentCount
	if v := os.Getenv("STREAMKIT_TEST_RECORDS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			count = parsed
		}
	} else if os.Getenv("STREAMKIT_TEST_SMALL") != "" || testing.Short() {
		count = 50
	}
	// Update global so tests can compute expected values
	IntegrationSegmentCount = count

	for i := range 5 {
		for j := range 5 {
			space, segment, records := fmt.Sprintf("space%d", i), fmt.Sprintf("segment%d", j), generateRange(0, count)
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
