package azurekit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	client "github.com/fgrzl/azkit/tables"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/streamkit/internal/cache"
	"github.com/fgrzl/streamkit/internal/codec"
	"github.com/fgrzl/streamkit/internal/txn"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/timestamp"
	"github.com/google/uuid"
)

// Constants
const (
	// Azure Table Storage batch transaction limit is 100 operations
	// All operations in a batch must share the EXACT same PartitionKey
	MaxBatchSize int = 100
	// Use smaller chunk size to account for encoding overhead and stay under 4MB per transaction
	BatchSize            int           = 90
	CacheTTL             time.Duration = time.Second * 97
	CacheCleanupInterval time.Duration = time.Second * 59
	ShutdownTimeout      time.Duration = time.Second * 59
	LAST_ENTRY           string        = "LAST_ENTRY"
	// Max payload size per Azure Table transaction (keep a cushion)
	MaxTransactionPayloadBytes int = 4*1024*1024 - 256*1024 // ~3.75MB
	// Number of parallel AddEntity operations within a payload-chunk
	AddWorkers int = 8
	// WAL monitoring interval for orphaned transactions
	WALMonitorInterval time.Duration = 5 * time.Minute
)

// Error Constants
const (
	ErrInvalidProduceArgs   = "invalid produce arguments"
	ErrPeekFailed           = "failed to peek"
	ErrTransactionCreate    = "failed to create transaction entity"
	ErrTransactionWrite     = "failed to write transaction"
	ErrTransactionFanout    = "failed to fanout transaction"
	ErrWALCleanup           = "failed to cleanup WAL"
	ErrBatchWrite           = "batch write failed"
	ErrSegmentInventory     = "failed to update segment inventory"
	ErrSpaceInventory       = "failed to update space inventory"
	ErrTableCreation        = "failed to create table"
	ErrUnmarshalEntity      = "failed to unmarshal entity"
	ErrDecodeEntry          = "failed to decode entry"
	ErrUnmarshalTransaction = "failed to unmarshal transaction"
	ErrBatchPrepare         = "failed to prepare batch"
	ErrStoreClosing         = "store is closing"
)

// Log Constants
const (
	LogWarnTimeoutTasks = "timeout waiting for tasks to complete"
	LogErrorFanout      = "failed to fanout transaction"
	LogErrorWALCleanup  = "failed to cleanup WAL"
)

// Types
type batchEntry struct {
	Entry        *api.Entry
	EncodedValue []byte
}

type committedInventoryError struct {
	cause error
}

func (e *committedInventoryError) Error() string {
	return e.cause.Error()
}

func (e *committedInventoryError) Unwrap() error {
	return e.cause
}

func NewAzureStore(ctx context.Context, client *client.HTTPTableClient, cache *cache.ExpiringCache, options *AzureStoreOptions) (*AzureStore, error) {
	store := &AzureStore{
		client:              client,
		cache:               cache,
		opts:                options,
		stopWALMonitor:      make(chan struct{}),
		walRecoveryComplete: make(chan struct{}),
	}

	// Validate and apply defaults
	if store.opts == nil {
		store.opts = &AzureStoreOptions{}
	}
	if store.opts.BatchSize == 0 {
		store.opts.BatchSize = BatchSize
	}
	if store.opts.AddWorkers == 0 {
		store.opts.AddWorkers = AddWorkers
	}
	if store.opts.MaxTransactionPayloadBytes == 0 {
		store.opts.MaxTransactionPayloadBytes = MaxTransactionPayloadBytes
	}

	if !store.opts.SkipTableCreation {
		if err := store.createTableIfNotExists(ctx); err != nil {
			slog.ErrorContext(ctx, "azure store: table creation failed — check auth, RBAC, and network config",
				"error", err,
				"account", client.AccountName(),
				"endpoint", client.Endpoint(),
				"table", client.TableName(),
				"auth_mode_bearer", client.UseBearerToken(),
			)
			return nil, fmt.Errorf("create table if not exists failed: %w", err)
		}
	}

	if err := store.recoverWAL(ctx); err != nil {
		return nil, fmt.Errorf("recover WAL failed: %w", err)
	}

	// Signal recovery complete before accepting writes
	close(store.walRecoveryComplete)

	// Start background WAL monitor for orphaned transactions
	go store.walMonitorLoop(ctx)

	return store, nil
}

// AzureStore invariants and guarantees:
//  1. Insert-only semantics for data entities (no silent upserts).
//  2. Fanout is at-least-once and is driven by WAL transactions; duplicates are expected
//     to be tolerated or deduped using sequence numbers.
//  3. Sequence numbers are assigned by the caller (store validates ordering).
//  4. LAST_ENTRY is advisory (written after fanout) and may be slightly stale during failures.
//  5. Cache is best-effort/non-durable and may be transiently stale on crashes.
//
// These invariants are intentionally explicit to avoid accidental correctness regressions.
// AzureStore invariants and guarantees:
//  1. Insert-only semantics for data entities (no silent upserts).
//  2. Fanout is at-least-once and is driven by WAL transactions; duplicates are expected
//     to be tolerated or deduped using sequence numbers.
//  3. Sequence numbers are assigned by the caller (store validates ordering).
//  4. LAST_ENTRY is advisory (written after fanout) and may be slightly stale during failures.
//  5. Cache is best-effort/non-durable and may be transiently stale on crashes.
//
// These invariants are intentionally explicit to avoid accidental correctness regressions.
type AzureStore struct {
	client              *client.HTTPTableClient
	cache               *cache.ExpiringCache
	wg                  sync.WaitGroup
	shuttingDown        atomic.Bool
	closeOnce           sync.Once
	opts                *AzureStoreOptions
	stopWALMonitor      chan struct{}
	walRecoveryComplete chan struct{}
}

// DiagnoseAuth exposes diagnostic information about the authentication
// configuration and tests connectivity against Azure Table Storage.
// Returns a structured diagnostic result with suggestions for fixing issues.
func (s *AzureStore) DiagnoseAuth(ctx context.Context) *client.AuthDiagnostic {
	return s.client.DiagnoseAuth(ctx)
}

func (s *AzureStore) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	// All spaces stored in single partition
	pKey := lexkey.Encode(api.INVENTORY, api.SPACES).ToHexString()
	query := fmt.Sprintf("PartitionKey eq '%s'", pKey)
	// Optimize: only select RowKey to reduce bandwidth
	selectColumns := "RowKey"
	// Optimize: limit page size for better performance with large result sets
	top := int32(1000)

	entities := NewAzureTableEnumerator(ctx, s.client.NewListEntitiesPager(query, selectColumns, top))

	return enumerators.Map(entities, func(e *client.Entity) (string, error) {
		return e.RowKey, nil
	})
}

func (s *AzureStore) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	ts := timestamp.GetTimestamp()
	bounds := calculateTimeBounds(ts, args.MinTimestamp, args.MaxTimestamp)

	// PartitionKey is constant for all entries in a space
	pKey := lexkey.Encode(api.DATA, api.SPACES, args.Space).ToHexString()

	// RowKey encodes timestamp + segment + sequence, so we filter on timestamp range
	var rLower, rUpper string
	if len(args.Offset) > 0 {
		rLower = args.Offset.ToHexString()
	} else {
		rLower = lexkey.EncodeFirst(bounds.Min).ToHexString()
	}
	rUpper = lexkey.EncodeLast(bounds.Max).ToHexString()

	query := fmt.Sprintf("PartitionKey eq '%s' and RowKey gt '%s' and RowKey le '%s'", pKey, rLower, rUpper)
	// Optimize: limit page size to reduce memory and improve response time
	top := int32(1000)

	entities := NewAzureTableEnumerator(ctx, s.client.NewListEntitiesPager(query, "", top))

	entries := enumerators.Map(entities, func(e *client.Entity) (*api.Entry, error) {
		return decodeEntry(e.Value)
	})

	// Note: MinTS is exclusive, MaxTS is inclusive
	return enumerators.TakeWhile(entries, func(e *api.Entry) bool {
		return e.Timestamp > bounds.Min && e.Timestamp <= bounds.Max
	})
}

func (s *AzureStore) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	// All segments for a space stored in single partition
	pKey := lexkey.Encode(api.INVENTORY, api.SEGMENTS, space).ToHexString()
	query := fmt.Sprintf("PartitionKey eq '%s'", pKey)
	// Optimize: only select RowKey to reduce bandwidth
	selectColumns := "RowKey"
	// Optimize: limit page size for better performance with large result sets
	top := int32(1000)

	entities := NewAzureTableEnumerator(ctx, s.client.NewListEntitiesPager(query, selectColumns, top))

	return enumerators.Map(entities, func(e *client.Entity) (string, error) {
		return e.RowKey, nil
	})
}

func (s *AzureStore) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	ts := timestamp.GetTimestamp()
	bounds := calculateSegmentBounds(ts, args)

	// All entries for a segment share the same partition key
	partitionKey := lexkey.Encode(api.DATA, api.SEGMENTS, args.Space, args.Segment).ToHexString()
	// Use Encode (not EncodeFirst) for inclusive lower bound - entry RowKey is Encode(seq)
	rLower := lexkey.Encode(bounds.MinSeq).ToHexString()
	rUpper := lexkey.EncodeLast(bounds.MaxSeq).ToHexString()

	query := fmt.Sprintf("PartitionKey eq '%s' and RowKey ge '%s' and RowKey le '%s'",
		partitionKey, rLower, rUpper)
	// Optimize: limit page size to reduce memory and improve response time
	top := int32(1000)

	entities := NewAzureTableEnumerator(ctx, s.client.NewListEntitiesPager(query, "", top))

	entries := enumerators.Map(entities, func(e *client.Entity) (*api.Entry, error) {
		return decodeEntry(e.Value)
	})

	// Filter entries that match the bounds
	// Note: MinSeq is inclusive, MaxSeq is inclusive, MinTS/MaxTS are exclusive/inclusive respectively
	filtered := enumerators.Filter(entries, func(e *api.Entry) bool {
		return e.Sequence >= bounds.MinSeq &&
			e.Sequence <= bounds.MaxSeq &&
			e.Timestamp > bounds.MinTS &&
			e.Timestamp <= bounds.MaxTS
	})

	return filtered
}

func (s *AzureStore) Peek(ctx context.Context, space, segment string) (*api.Entry, error) {
	cacheKey := fmt.Sprintf("peek:%s:%s", space, segment)
	if cached, ok := s.cache.Get(cacheKey); ok {
		if entry, ok := cached.(*api.Entry); ok {
			return entry, nil
		}
	}

	pk := lexkey.Encode(LAST_ENTRY, space, segment).ToHexString()
	rk := lexkey.Encode(lexkey.EndMarker).ToHexString()

	resp, err := s.client.GetEntity(ctx, pk, rk)
	if err != nil {
		if isNotFoundError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", ErrPeekFailed, err)
	}

	entry, err := decodeSnappyEntryEntity(resp)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ErrPeekFailed, err)
	}
	s.cache.Set(cacheKey, entry)
	return entry, nil
}

func (s *AzureStore) GetSegmentStatus(ctx context.Context, space, segment string) (*api.SegmentStatus, error) {
	status, err := s.readStoredSegmentStatus(ctx, space, segment)
	if err != nil || status != nil {
		return status, err
	}
	return s.buildSegmentStatusFromData(ctx, space, segment)
}

func (s *AzureStore) Produce(ctx context.Context, args *api.Produce, records enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	if args == nil || args.Space == "" || args.Segment == "" {
		return enumerators.Error[*api.SegmentStatus](errors.New(ErrInvalidProduceArgs))
	}
	if s.shuttingDown.Load() {
		return enumerators.Error[*api.SegmentStatus](errors.New(ErrStoreClosing))
	}

	// Wait for WAL recovery to complete before accepting writes
	select {
	case <-s.walRecoveryComplete:
		// Recovery complete, proceed
	case <-ctx.Done():
		return enumerators.Error[*api.SegmentStatus](ctx.Err())
	}

	lastEntry, err := s.Peek(ctx, args.Space, args.Segment)
	if err != nil {
		return enumerators.Error[*api.SegmentStatus](fmt.Errorf("%s: %w", ErrPeekFailed, err))
	}
	if lastEntry == nil {
		lastEntry = &api.Entry{Sequence: 0, TRX: api.TRX{Number: 0}}
	}

	chunks := enumerators.ChunkByCount(records, s.batchSize())
	var lastSeq, lastTrx = lastEntry.Sequence, lastEntry.TRX.Number

	return enumerators.Map(chunks, func(chunk enumerators.Enumerator[*api.Record]) (*api.SegmentStatus, error) {
		return s.processChunkWithRetry(ctx, args.Space, args.Segment, chunk, &lastSeq, &lastTrx)
	})
}

func (s *AzureStore) Close() {
	s.closeOnce.Do(func() {
		s.shuttingDown.Store(true)
		// Stop WAL monitor
		close(s.stopWALMonitor)
		if !s.waitForTasks(ShutdownTimeout) {
			slog.Warn(LogWarnTimeoutTasks)
		}
		s.cache.Close()
	})
}

// Private Instance Methods

func (s *AzureStore) processChunkWithRetry(ctx context.Context, space, segment string, chunk enumerators.Enumerator[*api.Record], lastSeq, lastTrx *uint64) (*api.SegmentStatus, error) {
	// Buffer the chunk into a slice so retries can replay the exact same records.
	records, err := enumerators.ToSlice(chunk)
	if err != nil {
		return nil, err
	}

	// If the buffered records, when encoded into a transaction, are too large,
	// split them into smaller sub-chunks and process sequentially to avoid exceeding
	// Azure entity size limits for the WAL transaction entity.
	return s.processBufferedChunk(ctx, space, segment, records, lastSeq, lastTrx)
}

// processBufferedChunk ensures that a slice of records is either small enough
// to be written as a single transaction, or is split and processed in smaller
// chunks. It returns the final SegmentStatus for the processed records and
// updates lastSeq/lastTrx accordingly.
func (s *AzureStore) processBufferedChunk(ctx context.Context, space, segment string, records []*api.Record, lastSeq, lastTrx *uint64) (*api.SegmentStatus, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("empty chunk unexpectedly")
	}

	// Helper to estimate encoded transaction size for a group of records
	estimateSize := func(recs []*api.Record, trx api.TRX, ls uint64) (int, error) {
		entries, err := createEntries(recs, space, segment, trx, ls)
		if err != nil {
			return 0, err
		}
		transaction := createTransaction(trx, space, segment, entries)
		b, err := codec.EncodeTransactionSnappy(transaction)
		if err != nil {
			return 0, err
		}
		return len(b), nil
	}

	// Choose trx for estimation without modifying the caller's counters
	trx := api.TRX{ID: uuid.New(), Number: *lastTrx + 1}
	sz, err := estimateSize(records, trx, *lastSeq)
	if err != nil {
		return nil, err
	}

	// If estimated size is too large, split and process halves recursively
	// Use a conservative threshold to account for encoding overhead in the WAL entity.
	if sz > MaxTransactionPayloadBytes/4 && len(records) > 1 {
		mid := len(records) / 2
		// Process left half
		if _, err := s.processBufferedChunk(ctx, space, segment, records[:mid], lastSeq, lastTrx); err != nil {
			return nil, err
		}
		// Process right half (lastSeq/lastTrx updated by left half)
		return s.processBufferedChunk(ctx, space, segment, records[mid:], lastSeq, lastTrx)
	}
	// If record slice encodes to more than the maximum allowed payload but is a single
	// record, return an error since it cannot be split.
	if sz > MaxTransactionPayloadBytes {
		return nil, fmt.Errorf("%s: transaction entity exceeds maximum allowed payload", ErrTransactionWrite)
	}

	// Otherwise perform the usual retry loop for this (now size-limited) slice
	const maxRetryAttempts = 3
	const initialRetryDelay = 100 * time.Millisecond
	const maxRetryDelay = 10 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		status, err := s.processChunk(ctx, space, segment, records, *lastSeq, *lastTrx)
		if err == nil {
			*lastSeq = status.LastSequence
			*lastTrx += 1
			return status, nil
		}
		var inventoryErr *committedInventoryError
		if errors.As(err, &inventoryErr) {
			return nil, inventoryErr
		}
		if !isRetryableError(err) {
			return nil, err
		}
		lastErr = err
		// Exponential backoff with jitter: 100ms, 200ms, 400ms, 800ms, 1600ms (capped at maxRetryDelay)
		if attempt < maxRetryAttempts-1 {
			backoff := initialRetryDelay * time.Duration(1<<uint(attempt))
			if backoff > maxRetryDelay {
				backoff = maxRetryDelay
			}
			// Add jitter: random 0-25% of backoff to prevent thundering herd
			jitter := time.Duration(float64(backoff) * 0.25 * float64(time.Now().UnixNano()%100) / 100.0)
			d := backoff + jitter
			select {
			case <-time.After(d):
				// continue retry
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	slog.ErrorContext(ctx, "azure store: exhausted retries processing chunk",
		slog.String("space", space),
		slog.String("segment", segment),
		slog.Int("record_count", len(records)),
		slog.Int("max_attempts", maxRetryAttempts),
		slog.String("last_error", lastErr.Error()))
	return nil, fmt.Errorf("failed after %d attempts: %w", maxRetryAttempts, lastErr)
}

func (s *AzureStore) processChunk(ctx context.Context, space, segment string, records []*api.Record, lastSeq, lastTrx uint64) (*api.SegmentStatus, error) {
	if !s.beginTask() {
		return nil, errors.New(ErrStoreClosing)
	}
	defer s.endTask()

	trx := api.TRX{ID: uuid.New(), Number: lastTrx + 1}
	entries, err := createEntries(records, space, segment, trx, lastSeq)
	if err != nil {
		return nil, err
	}
	transaction := createTransaction(trx, space, segment, entries)
	if err := s.executeTransaction(ctx, transaction); err != nil {
		return nil, err
	}

	// Update the Peek cache with the last entry we just wrote
	// This is more efficient than deleting and forcing a re-fetch
	cacheKey := fmt.Sprintf("peek:%s:%s", space, segment)
	lastEntry := entries[len(entries)-1]
	s.cache.Set(cacheKey, lastEntry)

	status := createSegmentStatus(space, segment, entries)
	mergedStatus, err := s.mergeStoredSegmentStatus(ctx, status)
	if err != nil {
		return nil, err
	}
	if err := s.writeSegmentStatus(ctx, mergedStatus); err != nil {
		return nil, &committedInventoryError{cause: err}
	}

	return mergedStatus, nil
}

func (s *AzureStore) beginTask() bool {
	if s.shuttingDown.Load() {
		return false
	}
	s.wg.Add(1)
	if s.shuttingDown.Load() {
		s.wg.Done()
		return false
	}
	return true
}

func (s *AzureStore) endTask() {
	s.wg.Done()
}

func (s *AzureStore) recoverWAL(ctx context.Context) error {
	// Note: This query scans across multiple partitions by design
	// Transaction PartitionKeys are: TRANSACTION + Space + Segment + TrxNumber
	// This is the only multi-partition query in the system and only runs at startup
	// Mitigation: transactions are cleaned up immediately after fanout, so the scan
	// should find very few (ideally zero) uncommitted transactions in normal operation
	lower, upper := lexkey.EncodeFirst(api.TRANSACTION).ToHexString(), lexkey.EncodeLast(api.TRANSACTION).ToHexString()
	query := fmt.Sprintf("PartitionKey ge '%s' and PartitionKey lt '%s'", lower, upper)
	// Optimize: limit page size for WAL recovery
	top := int32(1000)

	pager := s.client.NewListEntitiesPager(query, "", top)
	transactions := enumerators.Map(
		NewAzureTableEnumerator(ctx, pager),
		func(e *client.Entity) (*txn.Transaction, error) {
			transaction := &txn.Transaction{}
			// WAL stored Value is snappy-encoded transaction bytes; decode accordingly.
			if err := codec.DecodeTransactionSnappy(e.Value, transaction); err != nil {
				return nil, fmt.Errorf("%s: %w", ErrUnmarshalTransaction, err)
			}
			if err := s.fanoutTransaction(ctx, transaction); err != nil {
				slog.ErrorContext(ctx, LogErrorFanout,
					append(transactionLogFields(transaction),
						slog.String("wal_partition_key", e.PartitionKey),
						slog.String("wal_row_key", e.RowKey),
						slog.Any("err", err))...)
				return nil, err
			}
			invKey := fmt.Sprintf("inventory:%s:%s", transaction.Space, transaction.Segment)
			s.cache.Delete(invKey)
			if err := s.retryInventoryUpdate(ctx, transaction.Space, transaction.Segment); err != nil {
				return nil, err
			}
			if len(transaction.Entries) > 0 {
				status := createSegmentStatus(transaction.Space, transaction.Segment, transaction.Entries)
				mergedStatus, err := s.mergeStoredSegmentStatus(ctx, status)
				if err != nil {
					return nil, err
				}
				if err := s.writeSegmentStatus(ctx, mergedStatus); err != nil {
					return nil, err
				}
			}
			if err := s.cleanupWAL(ctx, e.PartitionKey, e.RowKey); err != nil {
				slog.ErrorContext(ctx, LogErrorWALCleanup,
					append(transactionLogFields(transaction),
						slog.String("wal_partition_key", e.PartitionKey),
						slog.String("wal_row_key", e.RowKey),
						slog.Any("err", err))...)
				return nil, err
			}
			if len(transaction.Entries) > 0 {
				peekKey := fmt.Sprintf("peek:%s:%s", transaction.Space, transaction.Segment)
				s.cache.Set(peekKey, transaction.Entries[len(transaction.Entries)-1])
			}
			return transaction, nil
		})

	return enumerators.Consume(transactions)
}

// walMonitorLoop periodically scans for orphaned WAL transactions and recovers them.
// This handles the case where a process crashes after writing WAL but before fanout completion.
// Issue #5 FIX: Use parent context instead of context.Background() to respect shutdown
func (s *AzureStore) walMonitorLoop(ctx context.Context) {
	// Issue #12: Add restart counter to prevent infinite goroutine bomb on persistent panics
	const maxRestarts = 5
	restartCount := 0

	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "WAL monitor panicked, restarting",
				slog.Any("panic", r), slog.Int("restarts", restartCount))
			if restartCount < maxRestarts {
				time.Sleep(time.Duration(restartCount+1) * time.Second) // Exponential backoff
				restartCount++
				go s.walMonitorLoopWithRestarts(ctx, restartCount)
			} else {
				slog.ErrorContext(ctx, "WAL monitor exceeded max restarts, stopping permanently",
					slog.Int("maxRestarts", maxRestarts))
			}
		}
	}()

	ticker := time.NewTicker(WALMonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopWALMonitor:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Create a timeout context derived from parent context, not from Background()
			// This ensures WAL recovery respects server shutdown
			recoveryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := s.recoverWAL(recoveryCtx); err != nil {
				slog.WarnContext(ctx, "WAL monitor: recovery failed",
					slog.Duration("recovery_timeout", 30*time.Second),
					slog.Duration("monitor_interval", WALMonitorInterval),
					"err", err)
				// Don't crash, retry next interval
			}
			cancel()
		}
	}
}

// walMonitorLoopWithRestarts is a helper for recursive restart with restart counter
func (s *AzureStore) walMonitorLoopWithRestarts(ctx context.Context, restartCount int) {
	const maxRestarts = 5

	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "WAL monitor panicked, restarting",
				slog.Any("panic", r), slog.Int("restarts", restartCount))
			if restartCount < maxRestarts {
				time.Sleep(time.Duration(restartCount+1) * time.Second)
				go s.walMonitorLoopWithRestarts(ctx, restartCount+1)
			} else {
				slog.ErrorContext(ctx, "WAL monitor exceeded max restarts, stopping permanently",
					slog.Int("maxRestarts", maxRestarts))
			}
		}
	}()

	ticker := time.NewTicker(WALMonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopWALMonitor:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			recoveryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := s.recoverWAL(recoveryCtx); err != nil {
				slog.WarnContext(ctx, "WAL monitor: recovery failed",
					slog.Duration("recovery_timeout", 30*time.Second),
					slog.Duration("monitor_interval", WALMonitorInterval),
					"err", err)
			}
			cancel()
		}
	}
}

func (s *AzureStore) executeTransaction(ctx context.Context, transaction *txn.Transaction) error {
	transactionEntity, err := createTransactionEntity(transaction)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrTransactionCreate, err)
	}

	data, err := marshalEntity(transactionEntity)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrTransactionWrite, err)
	}
	if err := s.client.AddEntity(ctx, data); err != nil {
		return fmt.Errorf("%s: %w", ErrTransactionWrite, err)
	}

	if err := s.fanoutTransaction(ctx, transaction); err != nil {
		return fmt.Errorf("%s: %w", ErrTransactionFanout, err)
	}

	// Invalidate affected caches after successful write
	peekKey := fmt.Sprintf("peek:%s:%s", transaction.Space, transaction.Segment)
	s.cache.Delete(peekKey)

	invKey := fmt.Sprintf("inventory:%s:%s", transaction.Space, transaction.Segment)
	s.cache.Delete(invKey)

	if err := s.retryInventoryUpdate(ctx, transaction.Space, transaction.Segment); err != nil {
		return &committedInventoryError{cause: err}
	}
	if err := s.cleanupWAL(ctx, transactionEntity.PartitionKey, transactionEntity.RowKey); err != nil {
		return fmt.Errorf("%s: %w", ErrWALCleanup, err)
	}

	return nil
}

func (s *AzureStore) cleanupWAL(ctx context.Context, pk, rk string) error {
	if err := s.client.DeleteEntity(ctx, pk, rk); err != nil {
		return fmt.Errorf("%s: %w", ErrWALCleanup, err)
	}
	return nil
}

func (s *AzureStore) fanoutTransaction(ctx context.Context, transaction *txn.Transaction) error {
	batch, err := prepareBatchEntries(transaction.Entries)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrBatchPrepare, err)
	}

	if len(batch) == 0 {
		return fmt.Errorf("%s: empty batch", ErrBatchPrepare)
	}

	// Run space and segment writes in parallel, wait for them to finish, then update LAST_ENTRY.
	errChan := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	go s.writeSegmentBatch(ctx, batch, errChan, &wg)
	go s.writeSpaceBatch(ctx, batch, errChan, &wg)

	wg.Wait()
	close(errChan)

	// Issue #18: Collect all errors instead of returning just the first one
	var errs []error
	for err := range errChan {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		// Use errors.Join to return all errors (Go 1.20+)
		return errors.Join(errs...)
	}

	// Only write LAST_ENTRY after segment & space batches succeeded to avoid exposing a last
	// pointer to entries that don't exist yet.
	if err := s.writeLastEntry(ctx, batch[len(batch)-1]); err != nil {
		return fmt.Errorf("%s: %w", ErrBatchWrite, err)
	}

	return nil
}

func (s *AzureStore) writeLastEntry(ctx context.Context, entry batchEntry) error {
	entity := client.Entity{
		PartitionKey: lexkey.Encode(LAST_ENTRY, entry.Entry.Space, entry.Entry.Segment).ToHexString(),
		RowKey:       lexkey.Encode(lexkey.EndMarker).ToHexString(),
		Value:        entry.EncodedValue,
	}

	data, err := marshalEntity(entity)
	if err != nil {
		return err
	}
	if err := s.client.UpsertEntity(ctx, data, "Replace"); err != nil {
		return err
	}
	return nil
}

func (s *AzureStore) writeSegmentBatch(ctx context.Context, entries []batchEntry, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	if len(entries) == 0 {
		return
	}

	// Write each entry individually since they share the same partition (space+segment)
	// but Azure batch transactions require all operations to target the EXACT same partition key
	partitionKey := lexkey.Encode(api.DATA, api.SEGMENTS, entries[0].Entry.Space, entries[0].Entry.Segment).ToHexString()

	entities := make([]client.Entity, len(entries))
	for i, entry := range entries {
		entities[i] = client.Entity{
			PartitionKey: partitionKey,
			RowKey:       lexkey.Encode(entry.Entry.Sequence).ToHexString(),
			Value:        entry.EncodedValue,
		}
	}

	if err := s.writeBatch(ctx, entities); err != nil {
		errChan <- err
	}
}

func (s *AzureStore) writeSpaceBatch(ctx context.Context, entries []batchEntry, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	if len(entries) == 0 {
		return
	}

	// All space entries for a given space share the same partition key
	// RowKey encodes: timestamp + segment + sequence for uniqueness and proper ordering
	partitionKey := lexkey.Encode(api.DATA, api.SPACES, entries[0].Entry.Space).ToHexString()

	entities := make([]client.Entity, len(entries))
	for i, entry := range entries {
		entities[i] = client.Entity{
			PartitionKey: partitionKey,
			RowKey:       lexkey.Encode(entry.Entry.Timestamp, entry.Entry.Segment, entry.Entry.Sequence).ToHexString(),
			Value:        entry.EncodedValue,
		}
	}

	if err := s.writeBatch(ctx, entities); err != nil {
		errChan <- err
	}
}

func (s *AzureStore) writeBatch(ctx context.Context, entities []client.Entity) error {
	if len(entities) == 0 {
		return nil
	}

	// Azure Table Storage batch transaction constraints:
	// 1. Max 100 operations per batch
	// 2. All operations must have the SAME PartitionKey
	// 3. Max 4MB per transaction
	if len(entities) > MaxBatchSize {
		// Process in chunks of MaxBatchSize
		for i := 0; i < len(entities); i += MaxBatchSize {
			end := i + MaxBatchSize
			if end > len(entities) {
				end = len(entities)
			}
			if err := s.writeBatch(ctx, entities[i:end]); err != nil {
				return err
			}
		}
		return nil
	}

	// Validate all entities share the same partition key
	partitionKey := entities[0].PartitionKey
	for i := 1; i < len(entities); i++ {
		if entities[i].PartitionKey != partitionKey {
			return fmt.Errorf("%s: partition key mismatch in batch - all operations must target same partition", ErrBatchWrite)
		}
	}

	// For correctness, perform insert-only writes but do so in payload-sized chunks
	// and in parallel per-chunk to improve throughput while avoiding Azure's 4MB per-transaction limit.
	// We marshal the entity to estimate payload size; if a single entity exceeds our payload
	// threshold it will be written on its own and may still fail due to service limits.
	var chunks [][]client.Entity
	var cur []client.Entity
	curSize := 0
	for _, e := range entities {
		b, err := marshalEntity(e)
		if err != nil {
			// If we can't marshal an entity, abort the batch with a clear error
			return fmt.Errorf("%s: %w", ErrBatchWrite, err)
		}
		sz := len(b)
		// Check BOTH entity count (max 100) AND payload size (max 4MB)
		if len(cur) > 0 && (len(cur) >= MaxBatchSize || (curSize+sz) > s.maxTransactionPayloadBytes()) {
			chunks = append(chunks, cur)
			cur = nil
			curSize = 0
		}
		cur = append(cur, e)
		curSize += sz
	}
	if len(cur) > 0 {
		chunks = append(chunks, cur)
	}

	// Use native Azure batch API for optimal performance
	// Each chunk can have different partition keys but up to 100 entities total
	// If all entities share the same partition key, Azure will atomically commit
	for _, chunk := range chunks {
		// Check context before processing each chunk
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Marshal all entities in this chunk
		entityData := make([][]byte, len(chunk))
		for i, en := range chunk {
			b, err := marshalEntity(en)
			if err != nil {
				return fmt.Errorf("%s: %w", ErrBatchWrite, err)
			}
			entityData[i] = b
		}

		// Use native Azure batch operation (1 HTTP request instead of N)
		if err := s.client.AddEntityBatch(ctx, entityData); err != nil {
			return fmt.Errorf("%s: %w", ErrBatchWrite, err)
		}
	}
	return nil
}

func (s *AzureStore) updateInventory(ctx context.Context, space, segment string) error {
	cacheKey := fmt.Sprintf("inventory:%s:%s", space, segment)

	// Fast check without lock (best effort)
	if _, ok := s.cache.Get(cacheKey); ok {
		return nil
	}

	// Write segment to space-specific partition
	segmentPK := lexkey.Encode(api.INVENTORY, api.SEGMENTS, space).ToHexString()
	data, err := marshalEntity(client.Entity{
		PartitionKey: segmentPK,
		RowKey:       segment,
	})
	if err != nil {
		return fmt.Errorf("%s: %w", ErrSegmentInventory, err)
	}
	if err := s.client.UpsertEntity(ctx, data, "Replace"); err != nil {
		return fmt.Errorf("%s: %w", ErrSegmentInventory, err)
	}

	// Write space to global spaces partition
	spacePK := lexkey.Encode(api.INVENTORY, api.SPACES).ToHexString()
	data, err = marshalEntity(client.Entity{
		PartitionKey: spacePK,
		RowKey:       space,
	})
	if err != nil {
		return fmt.Errorf("%s: %w", ErrSpaceInventory, err)
	}
	if err := s.client.UpsertEntity(ctx, data, "Replace"); err != nil {
		return fmt.Errorf("%s: %w", ErrSpaceInventory, err)
	}

	// Cache only after both writes succeed so a failed first attempt does not
	// poison future retries.
	s.cache.Set(cacheKey, struct{}{})
	return nil
}

func (s *AzureStore) readStoredSegmentStatus(ctx context.Context, space, segment string) (*api.SegmentStatus, error) {
	resp, err := s.client.GetEntity(ctx, segmentStatusPartitionKey(space), segment)
	if err != nil {
		if isNotFoundError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get segment status: %w", err)
	}

	var entity client.Entity
	if err := json.Unmarshal(resp, &entity); err != nil {
		return nil, fmt.Errorf("failed to decode segment status entity: %w", err)
	}

	status := &api.SegmentStatus{}
	if err := json.Unmarshal(entity.Value, status); err != nil {
		return nil, fmt.Errorf("failed to decode segment status: %w", err)
	}
	return status, nil
}

func (s *AzureStore) buildSegmentStatusFromData(ctx context.Context, space, segment string) (*api.SegmentStatus, error) {
	lastEntry, err := s.Peek(ctx, space, segment)
	if err != nil {
		return nil, err
	}
	if lastEntry == nil || lastEntry.Sequence == 0 {
		return nil, nil
	}

	enum := s.ConsumeSegment(ctx, &api.ConsumeSegment{
		Space:   space,
		Segment: segment,
	})
	defer enum.Dispose()
	if !enum.MoveNext() {
		return &api.SegmentStatus{
			Space:          space,
			Segment:        segment,
			FirstSequence:  lastEntry.Sequence,
			FirstTimestamp: lastEntry.Timestamp,
			LastSequence:   lastEntry.Sequence,
			LastTimestamp:  lastEntry.Timestamp,
		}, enum.Err()
	}

	firstEntry, err := enum.Current()
	if err != nil {
		return nil, err
	}
	if firstEntry == nil {
		firstEntry = lastEntry
	}

	return &api.SegmentStatus{
		Space:          space,
		Segment:        segment,
		FirstSequence:  firstEntry.Sequence,
		FirstTimestamp: firstEntry.Timestamp,
		LastSequence:   lastEntry.Sequence,
		LastTimestamp:  lastEntry.Timestamp,
	}, nil
}

func (s *AzureStore) mergeStoredSegmentStatus(ctx context.Context, chunkStatus *api.SegmentStatus) (*api.SegmentStatus, error) {
	if chunkStatus == nil {
		return nil, nil
	}
	if chunkStatus.FirstSequence <= 1 {
		return chunkStatus, nil
	}
	stored, err := s.readStoredSegmentStatus(ctx, chunkStatus.Space, chunkStatus.Segment)
	if err != nil {
		return nil, err
	}
	if stored == nil {
		if chunkStatus.FirstSequence <= 1 {
			return chunkStatus, nil
		}
		return s.buildSegmentStatusFromData(ctx, chunkStatus.Space, chunkStatus.Segment)
	}

	return &api.SegmentStatus{
		Space:          chunkStatus.Space,
		Segment:        chunkStatus.Segment,
		FirstSequence:  stored.FirstSequence,
		FirstTimestamp: stored.FirstTimestamp,
		LastSequence:   chunkStatus.LastSequence,
		LastTimestamp:  chunkStatus.LastTimestamp,
	}, nil
}

func (s *AzureStore) writeSegmentStatus(ctx context.Context, status *api.SegmentStatus) error {
	if status == nil {
		return nil
	}
	value, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("failed to encode segment status: %w", err)
	}
	data, err := marshalEntity(client.Entity{
		PartitionKey: segmentStatusPartitionKey(status.Space),
		RowKey:       status.Segment,
		Value:        value,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal segment status: %w", err)
	}
	if err := s.client.UpsertEntity(ctx, data, "Replace"); err != nil {
		return fmt.Errorf("failed to write segment status: %w", err)
	}
	return nil
}

func segmentStatusPartitionKey(space string) string {
	return lexkey.Encode(api.INVENTORY, api.SEGMENT_STATUSES, space).ToHexString()
}

func (s *AzureStore) retryInventoryUpdate(ctx context.Context, space, segment string) error {
	const maxRetryAttempts = 3
	const initialRetryDelay = 100 * time.Millisecond
	const maxRetryDelay = 10 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		err := s.updateInventory(ctx, space, segment)
		if err == nil {
			return nil
		}
		if !isRetryableError(err) {
			return err
		}
		lastErr = err
		if attempt == maxRetryAttempts-1 {
			break
		}

		backoff := initialRetryDelay * time.Duration(1<<uint(attempt))
		if backoff > maxRetryDelay {
			backoff = maxRetryDelay
		}
		jitter := time.Duration(float64(backoff) * 0.25 * float64(time.Now().UnixNano()%100) / 100.0)
		select {
		case <-time.After(backoff + jitter):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("failed after %d attempts: %w", maxRetryAttempts, lastErr)
}

func (s *AzureStore) waitForTasks(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.wg.Wait()
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (s *AzureStore) createTableIfNotExists(ctx context.Context) error {
	err := s.client.CreateTable(ctx)
	if err == nil {
		return nil
	}

	if strings.Contains(err.Error(), "TableAlreadyExists") {
		return nil
	}

	return fmt.Errorf("%s: %w", ErrTableCreation, err)
}

// Private Helper Functions

func createTransaction(trx api.TRX, space, segment string, entries []*api.Entry) *txn.Transaction {
	return &txn.Transaction{
		TRX:           trx,
		Space:         space,
		Segment:       segment,
		FirstSequence: entries[0].Sequence,
		LastSequence:  entries[len(entries)-1].Sequence,
		Entries:       entries,
		Timestamp:     timestamp.GetTimestamp(),
	}
}

func transactionLogFields(transaction *txn.Transaction) []any {
	if transaction == nil {
		return nil
	}

	return []any{
		slog.String("transaction_id", transaction.TRX.ID.String()),
		slog.Uint64("transaction_number", transaction.TRX.Number),
		slog.String("space", transaction.Space),
		slog.String("segment", transaction.Segment),
		slog.Uint64("first_sequence", transaction.FirstSequence),
		slog.Uint64("last_sequence", transaction.LastSequence),
		slog.Int("entry_count", len(transaction.Entries)),
	}
}

func createTransactionEntity(transaction *txn.Transaction) (*client.Entity, error) {
	value, err := codec.EncodeTransactionSnappy(transaction)
	if err != nil {
		return nil, err
	}

	return &client.Entity{
		PartitionKey: lexkey.Encode(api.TRANSACTION, transaction.Space, transaction.Segment, transaction.TRX.Number).ToHexString(),
		RowKey:       lexkey.Encode(lexkey.EndMarker).ToHexString(),
		Value:        value,
	}, nil
}

func createEntries(records []*api.Record, space, segment string, trx api.TRX, lastSeq uint64) ([]*api.Entry, error) {
	ts := timestamp.GetTimestamp()
	entries := make([]*api.Entry, 0, len(records))
	for _, r := range records {
		lastSeq++
		if r.Sequence != lastSeq {
			return nil, api.ERR_SEQUENCE_MISMATCH
		}
		entries = append(entries, &api.Entry{
			TRX:       trx,
			Space:     space,
			Segment:   segment,
			Sequence:  r.Sequence,
			Timestamp: ts,
			Payload:   r.Payload,
			Metadata:  r.Metadata,
		})
	}
	return entries, nil
}

func prepareBatchEntries(entries []*api.Entry) ([]batchEntry, error) {
	batch := make([]batchEntry, len(entries))
	for i, e := range entries {
		encoded, err := codec.EncodeEntrySnappy(e)
		if err != nil {
			return nil, err
		}
		batch[i] = batchEntry{Entry: e, EncodedValue: encoded}
	}
	return batch, nil
}

func createSegmentStatus(space, segment string, entries []*api.Entry) *api.SegmentStatus {
	return &api.SegmentStatus{
		Space:          space,
		Segment:        segment,
		FirstSequence:  entries[0].Sequence,
		FirstTimestamp: entries[0].Timestamp,
		LastSequence:   entries[len(entries)-1].Sequence,
		LastTimestamp:  entries[len(entries)-1].Timestamp,
	}
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	var azureErr *client.AzureError
	if errors.As(err, &azureErr) && azureErr != nil {
		return azureErr.StatusCode == http.StatusNotFound || azureErr.Code == "ResourceNotFound"
	}
	return strings.Contains(err.Error(), "ResourceNotFound") || strings.Contains(err.Error(), "status=404")
}

func isRetryableError(err error) bool {
	retryable, _ := classifyAzureError(err)
	return retryable
}

func classifyAzureError(err error) (retryable bool, reason string) {
	if err == nil {
		return false, "none"
	}
	if errors.Is(err, context.Canceled) {
		return false, "context_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false, "deadline_exceeded"
	}
	if errors.Is(err, net.ErrClosed) {
		return false, "network_closed"
	}

	var azureErr *client.AzureError
	if errors.As(err, &azureErr) && azureErr != nil {
		return classifyAzureHTTPStatus(azureErr.StatusCode)
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true, "network_timeout"
		}
		return true, "network"
	}

	if status, ok := extractHTTPStatusCode(err.Error()); ok {
		return classifyAzureHTTPStatus(status)
	}

	return false, "other"
}

func classifyAzureHTTPStatus(status int) (retryable bool, reason string) {
	switch status {
	case 408:
		return true, "http_408"
	case 429:
		return true, "http_429"
	case 500:
		return true, "http_500"
	case 502:
		return true, "http_502"
	case 503:
		return true, "http_503"
	case 504:
		return true, "http_504"
	case 400:
		return false, "http_400"
	case 401:
		return false, "http_401"
	case 403:
		return false, "http_403"
	case 404:
		return false, "http_404"
	case 409:
		return false, "http_409"
	case 410:
		return false, "http_410"
	default:
		if status >= 400 && status < 500 {
			return false, fmt.Sprintf("http_%d", status)
		}
		if status >= 500 && status < 600 {
			return false, fmt.Sprintf("http_%d", status)
		}
		return false, "other"
	}
}

func extractHTTPStatusCode(message string) (int, bool) {
	msg := strings.ToLower(strings.TrimSpace(message))
	if len(msg) >= 3 && isThreeDigitStatus(msg[:3]) {
		status, err := strconv.Atoi(msg[:3])
		if err == nil {
			return status, true
		}
	}

	for _, marker := range []string{"status=", "status "} {
		idx := strings.Index(msg, marker)
		if idx == -1 {
			continue
		}
		start := idx + len(marker)
		for start < len(msg) && msg[start] == ' ' {
			start++
		}
		end := start
		for end < len(msg) && msg[end] >= '0' && msg[end] <= '9' {
			end++
		}
		if end-start == 3 && isThreeDigitStatus(msg[start:end]) {
			status, err := strconv.Atoi(msg[start:end])
			if err == nil {
				return status, true
			}
		}
	}

	return 0, false
}

func isThreeDigitStatus(value string) bool {
	if len(value) != 3 {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func decodeSnappyEntryEntity(value []byte) (*api.Entry, error) {
	var entity client.Entity
	if err := json.Unmarshal(value, &entity); err != nil {
		return nil, fmt.Errorf("%s: %w", ErrUnmarshalEntity, err)
	}
	return decodeEntry(entity.Value)
}

func decodeEntry(value []byte) (*api.Entry, error) {
	entry := &api.Entry{}
	if err := codec.DecodeEntrySnappy(value, entry); err != nil {
		return nil, fmt.Errorf("%s: %w", ErrDecodeEntry, err)
	}
	return entry, nil
}

func marshalEntity(v interface{}) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal entity: %w", err)
	}
	return data, nil
}

// Configuration helpers
func (s *AzureStore) batchSize() int {
	if s.opts != nil && s.opts.BatchSize > 0 {
		return s.opts.BatchSize
	}
	return BatchSize
}

func (s *AzureStore) maxTransactionPayloadBytes() int {
	if s.opts != nil && s.opts.MaxTransactionPayloadBytes > 0 {
		return s.opts.MaxTransactionPayloadBytes
	}
	return MaxTransactionPayloadBytes
}

func calculateTimeBounds(current, min, max int64) struct{ Min, Max int64 } {
	bounds := struct{ Min, Max int64 }{Min: min}
	if min > current {
		bounds.Min = current
	}
	bounds.Max = max
	if max == 0 || max > current {
		bounds.Max = current
	}
	return bounds
}

func calculateSegmentBounds(ts int64, args *api.ConsumeSegment) struct {
	MinSeq, MaxSeq uint64
	MinTS, MaxTS   int64
} {
	bounds := struct {
		MinSeq, MaxSeq uint64
		MinTS, MaxTS   int64
	}{
		MinSeq: args.MinSequence,
		MaxSeq: args.MaxSequence,
		MinTS:  args.MinTimestamp,
	}
	if bounds.MinTS > ts {
		bounds.MinTS = ts
	}
	if args.MaxTimestamp == 0 || args.MaxTimestamp > ts {
		bounds.MaxTS = ts
	} else {
		bounds.MaxTS = args.MaxTimestamp
	}
	if bounds.MaxSeq == 0 {
		bounds.MaxSeq = math.MaxUint64
	} else if bounds.MaxSeq < bounds.MinSeq {
		bounds.MaxSeq = bounds.MinSeq
	}
	return bounds
}
