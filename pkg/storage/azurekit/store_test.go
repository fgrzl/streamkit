package azurekit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	client "github.com/fgrzl/azkit/tables"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/streamkit/internal/cache"
	"github.com/fgrzl/streamkit/internal/codec"
	"github.com/fgrzl/streamkit/internal/txn"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validBase64AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

type stubNetError struct {
	timeout   bool
	temporary bool
}

func (e stubNetError) Error() string {
	if e.timeout {
		return "network timeout"
	}
	return "network failure"
}

func (e stubNetError) Timeout() bool {
	return e.timeout
}

func (e stubNetError) Temporary() bool {
	return e.temporary
}

func newTestHTTPTableClient(t *testing.T, serverURL string) *client.HTTPTableClient {
	t.Helper()

	httpClient, err := client.NewHTTPTableClient("devstoreaccount1", validBase64AccountKey, "TestTable", false, serverURL)
	require.NoError(t, err)
	return httpClient
}

func TestShouldCreateEntriesWithSharedTimestampAndCopiedFields(t *testing.T) {
	trx := api.TRX{ID: uuid.New(), Number: 7}
	records := []*api.Record{
		{Sequence: 1, Payload: []byte("first"), Metadata: map[string]string{"k": "v"}},
		{Sequence: 2, Payload: []byte("second"), Metadata: map[string]string{"k2": "v2"}},
	}

	entries, err := createEntries(records, "space-a", "segment-a", trx, 0)

	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, trx, entries[0].TRX)
	assert.Equal(t, trx, entries[1].TRX)
	assert.Equal(t, uint64(1), entries[0].Sequence)
	assert.Equal(t, uint64(2), entries[1].Sequence)
	assert.Equal(t, "space-a", entries[0].Space)
	assert.Equal(t, "segment-a", entries[0].Segment)
	assert.Equal(t, []byte("first"), entries[0].Payload)
	assert.Equal(t, map[string]string{"k": "v"}, entries[0].Metadata)
	assert.Equal(t, entries[0].Timestamp, entries[1].Timestamp)
	assert.NotZero(t, entries[0].Timestamp)
}

func TestShouldReturnSequenceMismatchWhenCreateEntriesSeesGap(t *testing.T) {
	entries, err := createEntries([]*api.Record{{Sequence: 2}}, "space-a", "segment-a", api.TRX{ID: uuid.New(), Number: 1}, 0)

	require.ErrorIs(t, err, api.ERR_SEQUENCE_MISMATCH)
	assert.Nil(t, entries)
}

func TestShouldPrepareBatchEntriesWithRoundTripEncoding(t *testing.T) {
	entry := &api.Entry{
		TRX:       api.TRX{ID: uuid.New(), Number: 3},
		Space:     "space-a",
		Segment:   "segment-a",
		Sequence:  10,
		Timestamp: 1234,
		Payload:   []byte("payload"),
		Metadata:  map[string]string{"hello": "world"},
	}

	batch, err := prepareBatchEntries([]*api.Entry{entry})

	require.NoError(t, err)
	require.Len(t, batch, 1)
	assert.Same(t, entry, batch[0].Entry)

	decoded, err := decodeEntry(batch[0].EncodedValue)
	require.NoError(t, err)
	assert.Equal(t, entry.Sequence, decoded.Sequence)
	assert.Equal(t, entry.Timestamp, decoded.Timestamp)
	assert.Equal(t, entry.Space, decoded.Space)
	assert.Equal(t, entry.Segment, decoded.Segment)
	assert.Equal(t, entry.Payload, decoded.Payload)
	assert.Equal(t, entry.Metadata, decoded.Metadata)
}

func TestShouldCreateSegmentStatusFromFirstAndLastEntry(t *testing.T) {
	entries := []*api.Entry{
		{Sequence: 10, Timestamp: 1000},
		{Sequence: 11, Timestamp: 1100},
		{Sequence: 12, Timestamp: 1200},
	}

	status := createSegmentStatus("space-a", "segment-a", entries)

	assert.Equal(t, "space-a", status.Space)
	assert.Equal(t, "segment-a", status.Segment)
	assert.Equal(t, uint64(10), status.FirstSequence)
	assert.Equal(t, int64(1000), status.FirstTimestamp)
	assert.Equal(t, uint64(12), status.LastSequence)
	assert.Equal(t, int64(1200), status.LastTimestamp)
}

func TestShouldStripSpacePrefixWhenNormalizeSpaceOffsetRowKey(t *testing.T) {
	entry := &api.Entry{
		Space:     "space-a",
		Segment:   "segment-a",
		Sequence:  7,
		Timestamp: 1234,
	}

	rowKey := normalizeSpaceOffsetRowKey(entry.Space, entry.GetSpaceOffset())

	assert.Equal(t, lexkey.Encode(entry.Timestamp, entry.Segment, entry.Sequence).ToHexString(), rowKey)
}

func TestShouldCreateTransactionAndEntityWithExpectedMetadata(t *testing.T) {
	entries := []*api.Entry{
		{TRX: api.TRX{ID: uuid.New(), Number: 4}, Space: "space-a", Segment: "segment-a", Sequence: 5, Timestamp: 100},
		{TRX: api.TRX{ID: uuid.New(), Number: 4}, Space: "space-a", Segment: "segment-a", Sequence: 6, Timestamp: 100},
	}
	trxMeta := api.TRX{ID: uuid.New(), Number: 9}
	transaction := createTransaction(trxMeta, "space-a", "segment-a", entries)

	require.Equal(t, trxMeta, transaction.TRX)
	assert.Equal(t, uint64(5), transaction.FirstSequence)
	assert.Equal(t, uint64(6), transaction.LastSequence)
	assert.Equal(t, entries, transaction.Entries)
	assert.NotZero(t, transaction.Timestamp)

	entity, err := createTransactionEntity(transaction)
	require.NoError(t, err)
	assert.Equal(t, lexkey.Encode(api.TRANSACTION, transaction.Space, transaction.Segment, transaction.TRX.Number).ToHexString(), entity.PartitionKey)
	assert.Equal(t, lexkey.Encode(lexkey.EndMarker).ToHexString(), entity.RowKey)

	decoded := &txn.Transaction{}
	require.NoError(t, codec.DecodeTransactionSnappy(entity.Value, decoded))
	assert.Equal(t, transaction.TRX, decoded.TRX)
	assert.Equal(t, transaction.Space, decoded.Space)
	assert.Equal(t, transaction.Segment, decoded.Segment)
	assert.Equal(t, transaction.FirstSequence, decoded.FirstSequence)
	assert.Equal(t, transaction.LastSequence, decoded.LastSequence)
	assert.Len(t, decoded.Entries, len(transaction.Entries))
}

func TestShouldReturnStructuredTransactionLogFields(t *testing.T) {
	assert.Nil(t, transactionLogFields(nil))

	transaction := &txn.Transaction{
		TRX:           api.TRX{ID: uuid.New(), Number: 11},
		Space:         "space-a",
		Segment:       "segment-a",
		FirstSequence: 7,
		LastSequence:  8,
		Entries:       []*api.Entry{{Sequence: 7}, {Sequence: 8}},
	}
	fields := transactionLogFields(transaction)
	require.Len(t, fields, 7)

	transactionID, ok := fields[0].(slog.Attr)
	require.True(t, ok)
	assert.Equal(t, "transaction_id", transactionID.Key)

	entryCount, ok := fields[6].(slog.Attr)
	require.True(t, ok)
	assert.Equal(t, "entry_count", entryCount.Key)
	assert.Equal(t, int64(2), entryCount.Value.Int64())
}

func TestShouldRoundTripEntityMarshallingForSnappyEntries(t *testing.T) {
	entry := &api.Entry{
		TRX:       api.TRX{ID: uuid.New(), Number: 5},
		Space:     "space-a",
		Segment:   "segment-a",
		Sequence:  99,
		Timestamp: 4567,
		Payload:   []byte("payload"),
		Metadata:  map[string]string{"env": "test"},
	}
	encoded, err := codec.EncodeEntrySnappy(entry)
	require.NoError(t, err)

	data, err := marshalEntity(client.Entity{Value: encoded})
	require.NoError(t, err)

	decoded, err := decodeSnappyEntryEntity(data)
	require.NoError(t, err)
	assert.Equal(t, entry.Sequence, decoded.Sequence)
	assert.Equal(t, entry.Timestamp, decoded.Timestamp)
	assert.Equal(t, entry.Space, decoded.Space)
	assert.Equal(t, entry.Segment, decoded.Segment)
	assert.Equal(t, entry.Payload, decoded.Payload)
	assert.Equal(t, entry.Metadata, decoded.Metadata)
}

func TestShouldClassifyAzureErrorsAndConfigurationDefaults(t *testing.T) {
	assert.True(t, isNotFoundError(errors.New("ResourceNotFound: missing")))
	assert.False(t, isNotFoundError(errors.New("permission denied")))

	tests := []struct {
		name      string
		err       error
		retryable bool
		reason    string
	}{
		{name: "nil", err: nil, retryable: false, reason: "none"},
		{name: "context canceled", err: context.Canceled, retryable: false, reason: "context_canceled"},
		{name: "deadline exceeded", err: context.DeadlineExceeded, retryable: false, reason: "deadline_exceeded"},
		{name: "network closed", err: net.ErrClosed, retryable: false, reason: "network_closed"},
		{name: "network timeout", err: stubNetError{timeout: true, temporary: true}, retryable: true, reason: "network_timeout"},
		{name: "generic network error", err: stubNetError{}, retryable: true, reason: "network"},
		{name: "azure 408", err: &client.AzureError{StatusCode: http.StatusRequestTimeout}, retryable: true, reason: "http_408"},
		{name: "azure 429", err: &client.AzureError{StatusCode: http.StatusTooManyRequests}, retryable: true, reason: "http_429"},
		{name: "wrapped azure 503", err: fmt.Errorf("wrapped: %w", &client.AzureError{StatusCode: http.StatusServiceUnavailable}), retryable: true, reason: "http_503"},
		{name: "azure 500", err: &client.AzureError{StatusCode: http.StatusInternalServerError}, retryable: true, reason: "http_500"},
		{name: "azure 502", err: &client.AzureError{StatusCode: http.StatusBadGateway}, retryable: true, reason: "http_502"},
		{name: "azure 504", err: &client.AzureError{StatusCode: http.StatusGatewayTimeout}, retryable: true, reason: "http_504"},
		{name: "batch entity string status", err: errors.New("batch entity failures: status 503: unavailable"), retryable: true, reason: "http_503"},
		{name: "leading status string", err: errors.New("429 TooManyRequests"), retryable: true, reason: "http_429"},
		{name: "azure 400", err: &client.AzureError{StatusCode: http.StatusBadRequest}, retryable: false, reason: "http_400"},
		{name: "azure 401", err: &client.AzureError{StatusCode: http.StatusUnauthorized}, retryable: false, reason: "http_401"},
		{name: "azure 403", err: &client.AzureError{StatusCode: http.StatusForbidden}, retryable: false, reason: "http_403"},
		{name: "azure 404", err: &client.AzureError{StatusCode: http.StatusNotFound, Code: "ResourceNotFound"}, retryable: false, reason: "http_404"},
		{name: "azure 409", err: &client.AzureError{StatusCode: http.StatusConflict}, retryable: false, reason: "http_409"},
		{name: "non retryable string status", err: errors.New("status=400 bad request"), retryable: false, reason: "http_400"},
		{name: "unclassified", err: errors.New("permission denied"), retryable: false, reason: "other"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retryable, reason := classifyAzureError(tt.err)
			assert.Equal(t, tt.retryable, retryable)
			assert.Equal(t, tt.reason, reason)
			assert.Equal(t, tt.retryable, isRetryableError(tt.err))
		})
	}

	defaultStore := &AzureStore{}
	assert.Equal(t, BatchSize, defaultStore.batchSize())
	assert.Equal(t, MaxTransactionPayloadBytes, defaultStore.maxTransactionPayloadBytes())

	configuredStore := &AzureStore{opts: &AzureStoreOptions{BatchSize: 12, MaxTransactionPayloadBytes: 2048}}
	assert.Equal(t, 12, configuredStore.batchSize())
	assert.Equal(t, 2048, configuredStore.maxTransactionPayloadBytes())
}

func TestShouldExposeAzureStoreDiagnosticsSnapshot(t *testing.T) {
	c := cache.NewExpiringCache(50*time.Millisecond, 10*time.Millisecond)
	t.Cleanup(c.Close)

	store := &AzureStore{cache: c}
	store.shuttingDown.Store(true)
	store.activeTasks.Store(3)
	store.walMonitorRestarts.Store(2)

	snapshot := store.DiagnosticsSnapshot()

	assert.True(t, snapshot.ShuttingDown)
	assert.Equal(t, int64(3), snapshot.ActiveTasks)
	assert.Equal(t, int32(2), snapshot.WALMonitorRestarts)
	assert.Equal(t, int32(0), snapshot.CacheCleanupPanics)
}

func TestShouldTrackActiveTasksLifecycle(t *testing.T) {
	store := &AzureStore{}

	require.True(t, store.beginTask())
	assert.Equal(t, int64(1), store.activeTasks.Load())

	store.endTask()
	assert.Equal(t, int64(0), store.activeTasks.Load())

	store.shuttingDown.Store(true)
	assert.False(t, store.beginTask())
	assert.Equal(t, int64(0), store.activeTasks.Load())
}

func TestShouldCalculateAzureTimeAndSegmentBounds(t *testing.T) {
	spaceBounds := calculateTimeBounds(100, 200, 0)
	assert.Equal(t, int64(100), spaceBounds.Min)
	assert.Equal(t, int64(100), spaceBounds.Max)

	spaceBounds = calculateTimeBounds(100, 20, 70)
	assert.Equal(t, int64(20), spaceBounds.Min)
	assert.Equal(t, int64(70), spaceBounds.Max)

	segmentBounds := calculateSegmentBounds(100, &api.ConsumeSegment{
		MinSequence:  5,
		MinTimestamp: 200,
		MaxSequence:  0,
		MaxTimestamp: 0,
	})
	assert.Equal(t, uint64(5), segmentBounds.MinSeq)
	assert.Equal(t, uint64(math.MaxUint64), segmentBounds.MaxSeq)
	assert.Equal(t, int64(100), segmentBounds.MinTS)
	assert.Equal(t, int64(100), segmentBounds.MaxTS)

	segmentBounds = calculateSegmentBounds(100, &api.ConsumeSegment{
		MinSequence:  7,
		MinTimestamp: 10,
		MaxSequence:  9,
		MaxTimestamp: 80,
	})
	assert.Equal(t, uint64(7), segmentBounds.MinSeq)
	assert.Equal(t, uint64(9), segmentBounds.MaxSeq)
	assert.Equal(t, int64(10), segmentBounds.MinTS)
	assert.Equal(t, int64(80), segmentBounds.MaxTS)

	segmentBounds = calculateSegmentBounds(100, &api.ConsumeSegment{
		MinSequence:  10,
		MinTimestamp: 10,
		MaxSequence:  3,
		MaxTimestamp: 80,
	})
	assert.Equal(t, uint64(10), segmentBounds.MinSeq)
	assert.Equal(t, uint64(10), segmentBounds.MaxSeq)
	assert.Equal(t, int64(10), segmentBounds.MinTS)
	assert.Equal(t, int64(80), segmentBounds.MaxTS)
}

func TestShouldReturnDecodeAndMarshalErrorsForInvalidPayloads(t *testing.T) {
	_, err := decodeSnappyEntryEntity([]byte("not-json"))
	require.Error(t, err)
	assert.ErrorContains(t, err, ErrUnmarshalEntity)

	entityJSON, err := json.Marshal(client.Entity{Value: []byte("not-zstd")})
	require.NoError(t, err)

	_, err = decodeSnappyEntryEntity(entityJSON)
	require.Error(t, err)
	assert.ErrorContains(t, err, ErrDecodeEntry)

	_, err = decodeEntry([]byte("not-zstd"))
	require.Error(t, err)
	assert.ErrorContains(t, err, ErrDecodeEntry)

	_, err = marshalEntity(struct{ Invalid func() }{Invalid: func() {}})
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to marshal entity")
}

func TestShouldWaitForTasksUntilCompleteOrTimeout(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		store := &AzureStore{}
		store.wg.Add(1)
		go store.wg.Done()

		assert.True(t, store.waitForTasks(time.Second))
	})

	t.Run("timeout", func(t *testing.T) {
		store := &AzureStore{}
		store.wg.Add(1)

		assert.False(t, store.waitForTasks(10*time.Millisecond))
		store.wg.Done()
	})
}

func TestShouldRejectProduceWhenStoreIsClosing(t *testing.T) {
	store := &AzureStore{}
	store.shuttingDown.Store(true)

	results := store.Produce(
		context.Background(),
		&api.Produce{Space: "space-a", Segment: "segment-a"},
		enumerators.Slice([]*api.Record{{Sequence: 1}}),
	)

	_, err := enumerators.ToSlice(results)
	require.Error(t, err)
	assert.ErrorContains(t, err, ErrStoreClosing)
}

func TestShouldReturnStoreClosingWhenShuttingDownWhenProcessChunk(t *testing.T) {
	store := &AzureStore{}
	store.shuttingDown.Store(true)

	status, err := store.processChunk(context.Background(), "space-a", "segment-a", []*api.Record{{Sequence: 1}}, 0, 0)
	require.Error(t, err)
	assert.Nil(t, status)
	assert.ErrorContains(t, err, ErrStoreClosing)
}

func TestShouldReturnErrorWhenCreateTableReturnsResourceNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"odata.error":{"code":"ResourceNotFound","message":{"lang":"en-US","value":"account missing"}}}`))
	}))
	defer server.Close()

	store := &AzureStore{
		client: newTestHTTPTableClient(t, server.URL),
		cache:  cache.NewExpiringCache(CacheTTL, CacheCleanupInterval),
	}
	t.Cleanup(func() {
		store.cache.Close()
	})

	err := store.createTableIfNotExists(context.Background())

	require.Error(t, err)
	assert.ErrorContains(t, err, ErrTableCreation)
	assert.ErrorContains(t, err, "ResourceNotFound")
}

func TestShouldRetryInventoryUpdateAfterInitialFailure(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if requestCount == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("inventory write failed"))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	store := &AzureStore{
		client: newTestHTTPTableClient(t, server.URL),
		cache:  cache.NewExpiringCache(CacheTTL, CacheCleanupInterval),
	}
	t.Cleanup(func() {
		store.cache.Close()
	})

	err := store.updateInventory(context.Background(), "space-a", "segment-a")
	require.Error(t, err)
	assert.ErrorContains(t, err, ErrSegmentInventory)

	err = store.updateInventory(context.Background(), "space-a", "segment-a")
	require.NoError(t, err)
	assert.Equal(t, 3, requestCount)
	_, ok := store.cache.Get("inventory:space-a:segment-a")
	assert.True(t, ok)
}

func TestShouldNotReplayFanoutWhenInventoryRetrySucceeds(t *testing.T) {
	var mu sync.Mutex
	batchCalls := 0
	walCreates := 0
	walDeletes := 0
	lastEntryWrites := 0
	statusWrites := 0
	inventoryWrites := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/$batch":
			mu.Lock()
			batchCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("HTTP/1.1 204 No Content\r\n\r\n"))

		case r.Method == http.MethodPost && r.URL.Path == "/TestTable":
			mu.Lock()
			walCreates++
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)

		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/TestTable("):
			mu.Lock()
			walDeletes++
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/TestTable("):
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)

			var payload map[string]any
			require.NoError(t, json.Unmarshal(body, &payload))

			if _, hasValue := payload["Value"]; hasValue {
				partitionKey, _ := payload["PartitionKey"].(string)
				if partitionKey == segmentStatusPartitionKey("space-a") {
					mu.Lock()
					statusWrites++
					mu.Unlock()
					w.WriteHeader(http.StatusNoContent)
					return
				}
				mu.Lock()
				lastEntryWrites++
				mu.Unlock()
				w.WriteHeader(http.StatusNoContent)
				return
			}

			mu.Lock()
			inventoryWrites++
			currentInventoryWrite := inventoryWrites
			mu.Unlock()

			if currentInventoryWrite == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("transient inventory failure"))
				return
			}

			w.WriteHeader(http.StatusNoContent)

		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	store := &AzureStore{
		client: newTestHTTPTableClient(t, server.URL),
		cache:  cache.NewExpiringCache(CacheTTL, CacheCleanupInterval),
	}
	t.Cleanup(func() {
		store.cache.Close()
	})

	lastSeq, lastTrx := uint64(0), uint64(0)
	status, err := store.processChunkWithRetry(
		context.Background(),
		"space-a",
		"segment-a",
		enumerators.Slice([]*api.Record{{Sequence: 1, Payload: []byte("payload")}}),
		&lastSeq,
		&lastTrx,
	)

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Equal(t, uint64(1), status.LastSequence)
	assert.Equal(t, uint64(1), lastSeq)
	assert.Equal(t, uint64(1), lastTrx)
	assert.Equal(t, 2, batchCalls, "inventory retries must not replay data fanout")
	assert.Equal(t, 1, walCreates, "inventory retries must not rewrite WAL")
	assert.Equal(t, 1, walDeletes, "inventory retries must not rerun WAL cleanup")
	assert.Equal(t, 1, lastEntryWrites, "inventory retries must not rewrite the last-entry marker")
	assert.Equal(t, 1, statusWrites, "inventory retries should persist one segment-status snapshot")
	assert.Equal(t, 3, inventoryWrites, "inventory retry should re-attempt the failed segment write and then complete the space write")
}

func TestShouldRebuildInventoryWhenRecoveringWAL(t *testing.T) {
	entry := &api.Entry{
		TRX:       api.TRX{ID: uuid.New(), Number: 1},
		Space:     "space-a",
		Segment:   "segment-a",
		Sequence:  1,
		Timestamp: 1234,
		Payload:   []byte("payload"),
	}
	transaction := &txn.Transaction{
		TRX:           entry.TRX,
		Space:         entry.Space,
		Segment:       entry.Segment,
		FirstSequence: entry.Sequence,
		LastSequence:  entry.Sequence,
		Entries:       []*api.Entry{entry},
		Timestamp:     entry.Timestamp,
	}
	walEntity, err := createTransactionEntity(transaction)
	require.NoError(t, err)

	var mu sync.Mutex
	batchCalls := 0
	walDeletes := 0
	lastEntryWrites := 0
	statusWrites := 0
	inventoryWrites := 0
	listCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/TestTable":
			mu.Lock()
			listCalls++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			response, marshalErr := json.Marshal(map[string]any{"value": []client.Entity{*walEntity}})
			require.NoError(t, marshalErr)
			_, _ = w.Write(response)

		case r.Method == http.MethodPost && r.URL.Path == "/$batch":
			mu.Lock()
			batchCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("HTTP/1.1 204 No Content\r\n\r\n"))

		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/TestTable("):
			mu.Lock()
			walDeletes++
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/TestTable("):
			body, readErr := io.ReadAll(r.Body)
			require.NoError(t, readErr)

			var payload map[string]any
			require.NoError(t, json.Unmarshal(body, &payload))

			if _, hasValue := payload["Value"]; hasValue {
				partitionKey, _ := payload["PartitionKey"].(string)
				if partitionKey == segmentStatusPartitionKey("space-a") {
					mu.Lock()
					statusWrites++
					mu.Unlock()
					w.WriteHeader(http.StatusNoContent)
					return
				}
				mu.Lock()
				lastEntryWrites++
				mu.Unlock()
				w.WriteHeader(http.StatusNoContent)
				return
			}

			mu.Lock()
			inventoryWrites++
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)

		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	store := &AzureStore{
		client: newTestHTTPTableClient(t, server.URL),
		cache:  cache.NewExpiringCache(CacheTTL, CacheCleanupInterval),
	}
	t.Cleanup(func() {
		store.cache.Close()
	})

	err = store.recoverWAL(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 1, listCalls, "WAL recovery should query the outstanding transactions once")
	assert.Equal(t, 2, batchCalls, "WAL recovery should replay both segment and space batches")
	assert.Equal(t, 1, walDeletes, "WAL recovery should delete the recovered WAL record")
	assert.Equal(t, 1, lastEntryWrites, "WAL recovery should restore the last-entry marker")
	assert.Equal(t, 1, statusWrites, "WAL recovery should rebuild the segment-status snapshot")
	assert.Equal(t, 2, inventoryWrites, "WAL recovery should rebuild segment and space inventory")
}

func TestShouldRefreshPeekCacheWhenRecoveringWAL(t *testing.T) {
	recoveredEntry := &api.Entry{
		TRX:       api.TRX{ID: uuid.New(), Number: 2},
		Space:     "space-a",
		Segment:   "segment-a",
		Sequence:  2,
		Timestamp: 2345,
		Payload:   []byte("recovered"),
	}
	transaction := &txn.Transaction{
		TRX:           recoveredEntry.TRX,
		Space:         recoveredEntry.Space,
		Segment:       recoveredEntry.Segment,
		FirstSequence: recoveredEntry.Sequence,
		LastSequence:  recoveredEntry.Sequence,
		Entries:       []*api.Entry{recoveredEntry},
		Timestamp:     recoveredEntry.Timestamp,
	}
	walEntity, err := createTransactionEntity(transaction)
	require.NoError(t, err)
	storedStatusJSON, err := json.Marshal(&api.SegmentStatus{
		Space:          "space-a",
		Segment:        "segment-a",
		FirstSequence:  1,
		FirstTimestamp: 1234,
		LastSequence:   1,
		LastTimestamp:  1234,
	})
	require.NoError(t, err)
	statusWrites := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/TestTable":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			response, marshalErr := json.Marshal(map[string]any{"value": []client.Entity{*walEntity}})
			require.NoError(t, marshalErr)
			_, _ = w.Write(response)

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/TestTable("):
			entityResponse, marshalErr := json.Marshal(client.Entity{
				PartitionKey: segmentStatusPartitionKey("space-a"),
				RowKey:       "segment-a",
				Value:        storedStatusJSON,
			})
			require.NoError(t, marshalErr)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(entityResponse)

		case r.Method == http.MethodPost && r.URL.Path == "/$batch":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("HTTP/1.1 204 No Content\r\n\r\n"))

		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/TestTable("):
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/TestTable("):
			body, readErr := io.ReadAll(r.Body)
			require.NoError(t, readErr)

			var payload map[string]any
			require.NoError(t, json.Unmarshal(body, &payload))

			if partitionKey, _ := payload["PartitionKey"].(string); partitionKey == segmentStatusPartitionKey("space-a") {
				statusWrites++
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	store := &AzureStore{
		client: newTestHTTPTableClient(t, server.URL),
		cache:  cache.NewExpiringCache(CacheTTL, CacheCleanupInterval),
	}
	t.Cleanup(func() {
		store.cache.Close()
	})

	staleEntry := &api.Entry{
		TRX:       api.TRX{ID: uuid.New(), Number: 1},
		Space:     "space-a",
		Segment:   "segment-a",
		Sequence:  1,
		Timestamp: 1234,
		Payload:   []byte("stale"),
	}
	store.cache.Set("peek:space-a:segment-a", staleEntry)

	err = store.recoverWAL(context.Background())
	require.NoError(t, err)

	peeked, err := store.Peek(context.Background(), "space-a", "segment-a")
	require.NoError(t, err)
	require.NotNil(t, peeked)
	assert.Equal(t, 1, statusWrites, "WAL recovery should refresh the persisted segment-status snapshot")
	assert.Equal(t, uint64(2), peeked.Sequence, "WAL recovery should replace stale peek cache entries")
	assert.Equal(t, []byte("recovered"), peeked.Payload)
}
