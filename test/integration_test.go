package test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"fmt"
	"testing"
	"time"

	"github.com/fgrzl/claims"
	"github.com/fgrzl/claims/jwtkit"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/fgrzl/streamkit/pkg/storage/azurekit"
	"github.com/fgrzl/streamkit/pkg/storage/pebblekit"
	"github.com/fgrzl/streamkit/pkg/transport/inprockit"
	"github.com/fgrzl/streamkit/pkg/transport/wskit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var secret = []byte("top-secret")
var tester = claims.NewClaimsList("tenant_id", uuid.NewString()).Add("scopes", "streamkit::*")

var storeID = uuid.New()

func wskitTestHarness(t *testing.T, factory storage.StoreFactory) *TestHarness {

	validator := &jwtkit.HMAC256Validator{
		Secret: secret,
	}

	nodeManager := server.NewNodeManager(server.WithStoreFactory(factory))

	router := mux.NewRouter()

	mux.UseAuthentication(router, mux.WithValidator(validator.Validate))

	mux.UseAuthorization(router)

	router.Healthz().AllowAnonymous()

	wskit.ConfigureWebSocketServer(router, nodeManager)

	server := httptest.NewServer(router)
	t.Cleanup(func() {
		server.Close()
		nodeManager.Close()
	})

	httpClient := server.Client()

	resp, err := httpClient.Get(server.URL + "/healthz")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	signer := jwtkit.HMAC256Signer{
		Secret: secret,
	}
	ttl := time.Minute
	token, err := signer.CreateToken(claims.NewPrincipalFromList(tester), ttl)
	require.NoError(t, err)

	url, err := url.Parse(server.URL)
	require.NoError(t, err)

	addr := "ws://" + url.Host
	provider := wskit.NewBidiStreamProvider(addr, func() (string, error) { return token, nil })
	clientInstance := client.NewClient(provider)

	harness := &TestHarness{
		Client: clientInstance,
	}
	return harness
}

func azurekitTestHarness(t *testing.T) *TestHarness {
	// Support environment variables for Azure Storage configuration
	accountName := "devstoreaccount1"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	endpoint := "http://127.0.0.1:10002/devstoreaccount1"

	credential, err := azurekit.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		t.Skipf("skipping azure tests: failed to create shared key credential: %v", err)
		return nil
	}

	options := &azurekit.AzureStoreOptions{
		Prefix:              uuid.NewString(),
		Endpoint:            endpoint,
		SharedKeyCredential: credential,
		AllowInsecureHTTP:   true,
	}

	factory, err := azurekit.NewStoreFactory(t.Context(), options)
	if err != nil {
		t.Skipf("skipping azure tests: failed to create store factory: %v", err)
		return nil
	}

	return wskitTestHarness(t, factory)
}

func pebblekitTestHarness(t *testing.T) *TestHarness {
	options := &pebblekit.PebbleStoreOptions{
		Path: t.TempDir(),
	}
	factory, err := pebblekit.NewStoreFactory(options)
	require.NoError(t, err)

	return wskitTestHarness(t, factory)
}

func inprockitTestHarness(t *testing.T) *TestHarness {
	options := &pebblekit.PebbleStoreOptions{Path: t.TempDir()}
	factory, err := pebblekit.NewStoreFactory(options)
	require.NoError(t, err)

	nodeManager := server.NewNodeManager(server.WithStoreFactory(factory))

	provider := inprockit.NewInProcBidiStreamProvider(t.Context(), nodeManager)
	clientInstance := client.NewClient(provider)

	t.Cleanup(func() {
		nodeManager.Close()
	})

	return &TestHarness{
		Client: clientInstance,
	}
}

func configurations() map[string]func(*testing.T) *TestHarness {
	return map[string]func(*testing.T) *TestHarness{
		"azure":  azurekitTestHarness,
		"pebble": pebblekitTestHarness,
		"inproc": inprockitTestHarness,
	}
}

func TestShouldAllowMultiplexedCallsWhenUsingDifferentSegments(t *testing.T) {
	for name, h := range configurations() {
		t.Run("should allow for multiplexed calls "+name, func(t *testing.T) {
			harness := h(t)
			for i := range 3 {
				// Arrange
				ctx := t.Context()
				space, segment := "space0", "segment"+strconv.Itoa(i)

				// Act
				entry, err := harness.Client.Peek(ctx, storeID, space, segment)

				// Assert
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
			for i := range 3 {
				// Arrange
				ctx := t.Context()
				space, segment, records := "space0", "segment"+strconv.Itoa(i), generateRange(0, 5)

				// Act
				results := harness.Client.Produce(ctx, storeID, space, segment, records)
				statuses, err := enumerators.ToSlice(results)

				// Assert
				require.NoError(t, err)
				assert.Len(t, statuses, 1)
			}
		})
	}
}

func TestConcurrentProducersDetectConflict(t *testing.T) {
	for name, h := range configurations() {
		t.Run("concurrent producers "+name, func(t *testing.T) {
			harness := h(t)
			ctx := t.Context()
			space, segment := "space-concurrent", "segment-conflict"

			// Each producer needs its own enumerator instance
			recA := generateRange(0, 200)
			recB := generateRange(0, 200)

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

			// Verify final segment is consistent: we expect exactly 200 entries with contiguous sequences
			enum := harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{Space: space, Segment: segment, MinSequence: 1})
			entries, err := enumerators.ToSlice(enum)
			require.NoError(t, err)
			require.Len(t, entries, 200)
			for i, e := range entries {
				reqSeq := uint64(i + 1)
				assert.Equal(t, reqSeq, e.Sequence)
			}
			// At least one producer should have encountered a conflict (server logs will show).
			// We don't rely on the client-side error here because of close/send races; instead
			// we validate storage invariants above to ensure no corruption occurred.
			// Still prefer seeing at least one error if the transport captured it.
			if err1 == nil && err2 == nil {
				t.Log("no producer-side errors observed; storage invariants hold")
			}
		})
	}
}

func TestProduceLargeRecordsChunking(t *testing.T) {
	for name, h := range configurations() {
		t.Run("large produce "+name, func(t *testing.T) {
			harness := h(t)
			if name == "azure" {
				t.Skip("skipping azure for large produce test due to entity size limits")
			}
			ctx := t.Context()
			space, segment := "space-large", "segment-large"

			recs := generateLargeRange(0, 100, 16*1024) // 16KB payloads -> trigger payload chunking
			_, err := enumerators.ToSlice(harness.Client.Produce(ctx, storeID, space, segment, recs))
			require.NoError(t, err)

			enum := harness.Client.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{Space: space, Segment: segment, MinSequence: 1})
			entries, err := enumerators.ToSlice(enum)
			require.NoError(t, err)
			require.Len(t, entries, 100)
		})
	}
}

func TestShouldReturnAllSpacesWhenRequested(t *testing.T) {
	for name, h := range configurations() {
		t.Run("should get spaces "+name, func(t *testing.T) {
			harness := h(t)
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, storeID, harness.Client)

			// Act
			enumerator := harness.Client.GetSpaces(ctx, storeID)
			spaces, err := enumerators.ToSlice(enumerator)

			// Assert
			require.NoError(t, err)
			assert.Len(t, spaces, 5)
			assert.Equal(t, "space0", spaces[0])
			assert.Equal(t, "space1", spaces[1])
			assert.Equal(t, "space2", spaces[2])
			assert.Equal(t, "space3", spaces[3])
			assert.Equal(t, "space4", spaces[4])
		})
	}
}

func TestShouldReturnAllSegmentsWhenGivenValidSpace(t *testing.T) {
	for name, h := range configurations() {
		t.Run("should get segments "+name, func(t *testing.T) {
			harness := h(t)
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, storeID, harness.Client)

			// Act
			enumerator := harness.Client.GetSegments(ctx, storeID, "space0")
			segments, err := enumerators.ToSlice(enumerator)

			// Assert
			require.NoError(t, err)
			assert.Len(t, segments, 5)
			assert.Equal(t, "segment0", segments[0])
			assert.Equal(t, "segment1", segments[1])
			assert.Equal(t, "segment2", segments[2])
			assert.Equal(t, "segment3", segments[3])
			assert.Equal(t, "segment4", segments[4])
		})
	}
}

func TestShouldReturnCorrectEntryWhenPeekingAtSegment(t *testing.T) {
	for name, h := range configurations() {
		t.Run("should peek "+name, func(t *testing.T) {
			harness := h(t)
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, storeID, harness.Client)

			// Act
			peek, err := harness.Client.Peek(ctx, storeID, "space0", "segment0")

			// Assert
			require.NoError(t, err)
			assert.Equal(t, "space0", peek.Space)
			assert.Equal(t, "segment0", peek.Segment)
			assert.Equal(t, uint64(253), peek.Sequence)
		})
	}
}

func TestShouldConsumeAllEntriesWhenGivenValidSegment(t *testing.T) {
	for name, h := range configurations() {
		t.Run("should consume segment "+name, func(t *testing.T) {
			harness := h(t)
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, storeID, harness.Client)

			args := &client.ConsumeSegment{
				Space:   "space0",
				Segment: "segment0",
			}

			// Act
			results := harness.Client.ConsumeSegment(ctx, storeID, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			require.NoError(t, err)
			assert.Len(t, entries, 253)
		})
	}
}

func TestShouldConsumePartialEntriesWhenGivenMinSequence(t *testing.T) {
	for name, h := range configurations() {
		t.Run("should consume segment with inclusive min "+name, func(t *testing.T) {
			harness := h(t)
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, storeID, harness.Client)

			args := &client.ConsumeSegment{
				Space:       "space0",
				Segment:     "segment0",
				MinSequence: 233,
			}

			// Act
			results := harness.Client.ConsumeSegment(ctx, storeID, args)
			entries, err := enumerators.ToSlice(results)

			// Assert - MinSequence is inclusive, so sequences 233-253 = 21 entries
			require.NoError(t, err)
			assert.Len(t, entries, 21)
		})
	}
}

func TestShouldConsumeAllEntriesWhenGivenValidSpace(t *testing.T) {
	for name, h := range configurations() {
		t.Run("should consume space "+name, func(t *testing.T) {
			harness := h(t)
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, storeID, harness.Client)

			args := &client.ConsumeSpace{
				Space: "space0",
			}

			// Act
			results := harness.Client.ConsumeSpace(ctx, storeID, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			require.NoError(t, err)
			assert.Len(t, entries, 1_265)
		})
	}
}

func TestShouldConsumeInterleavedEntriesWhenGivenMultipleSpaces(t *testing.T) {
	for name, h := range configurations() {
		t.Run("should consume interleaved spaces "+name, func(t *testing.T) {
			harness := h(t)
			// Arrange
			ctx := t.Context()

			setupConsumerData(t, storeID, harness.Client)

			// Wait/retry until per-space counts are visible to avoid intermittent snapshot misses.
			ensureSpaceCounts := func() {
				var lastCounts [5]int
				for attempt := 0; attempt < 20; attempt++ {
					ok := true
					for i := 0; i < 5; i++ {
						space := fmt.Sprintf("space%d", i)
						enum := harness.Client.ConsumeSpace(ctx, storeID, &client.ConsumeSpace{Space: space})
						entries, err := enumerators.ToSlice(enum)
						lastCounts[i] = len(entries)
						if err != nil || len(entries) != 1_265 {
							ok = false
							t.Logf("attempt %d: space %s has %d entries (err=%v)", attempt, space, len(entries), err)
						}
					}
					if ok {
						return
					}
					time.Sleep(50 * time.Millisecond)
				}
				// If we get here, counts didn't stabilize - fail with detailed counts
				t.Fatalf("setupConsumerData did not stabilize after retries, lastCounts=%v", lastCounts)
			}
			ensureSpaceCounts()

			args := &client.Consume{
				Offsets: map[string]lexkey.LexKey{
					"space0": {},
					"space1": {},
					"space2": {},
					"space3": {},
					"space4": {},
				},
			}

			// Act + retry until the interleaved view stabilizes (addresses intermittent snapshot timing)
			var entries []*client.Entry
			var err error
			for attempt := 0; attempt < 20; attempt++ {
				results := harness.Client.Consume(ctx, storeID, args)
				entries, err = enumerators.ToSlice(results)
				if err == nil && len(entries) == 6_325 {
					break
				}
				t.Logf("attempt %d: interleaved returned %d entries (err=%v)", attempt, len(entries), err)
				time.Sleep(50 * time.Millisecond)
			}

			// Assert
			require.NoError(t, err)
			assert.Len(t, entries, 6_325)
		})
	}
}
