package azurekit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
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
	BatchSize            int           = 100
	CacheTTL             time.Duration = time.Second * 97
	CacheCleanupInterval time.Duration = time.Second * 59
	ShutdownTimeout      time.Duration = time.Second * 59
	InitialRetryDelay    time.Duration = time.Millisecond * 100
	MaxRetryAttempts     int           = 3
	LAST_ENTRY           string        = "LAST_ENTRY"
)

// Error Constants
const (
	ErrInvalidProduceArgs   = "invalid produce arguments"
	ErrClientCreation       = "failed to create client"
	ErrTableInit            = "failed to initialize table"
	ErrWALRecovery          = "failed to recover WAL"
	ErrPeekFailed           = "failed to peek"
	ErrTransactionCreate    = "failed to create transaction entity"
	ErrTransactionWrite     = "failed to write transaction"
	ErrTransactionFanout    = "failed to fanout transaction"
	ErrWALCleanup           = "failed to cleanup WAL"
	ErrBatchWrite           = "batch write failed"
	ErrSegmentInventory     = "failed to update segment inventory"
	ErrSpaceInventory       = "failed to update space inventory"
	ErrTableCreation        = "failed to create table"
	ErrInvalidCredentials   = "please provide a valid Azure credential"
	ErrUnmarshalEntity      = "failed to unmarshal entity"
	ErrDecodeEntry          = "failed to decode entry"
	ErrUnmarshalTransaction = "failed to unmarshal transaction"
	ErrBatchPrepare         = "failed to prepare batch"
	ErrNotifySupervisor     = "failed to notify supervisor"
	ErrTimeoutTasks         = "timeout waiting for tasks to complete"
)

// Log Constants
const (
	LogWarnTimeoutTasks      = "timeout waiting for tasks to complete"
	LogErrorFanout           = "failed to fanout transaction"
	LogErrorWALCleanup       = "failed to cleanup WAL"
	LogErrorNotifySupervisor = "failed to notify supervisor"
)

// Types
type entity struct {
	PartitionKey string `json:"PartitionKey"`
	RowKey       string `json:"RowKey"`
	Value        []byte `json:"Value,omitempty"`
}

type batchEntry struct {
	Entry        *api.Entry
	EncodedValue []byte
}

func NewAzureStore(ctx context.Context, client *aztables.Client, cache *cache.ExpiringCache) (*AzureStore, error) {
	store := &AzureStore{
		client: client,
		cache:  cache,
	}

	if err := store.createTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("create table if not exists failed: %w", err)
	}

	if err := store.recoverWAL(ctx); err != nil {
		return nil, fmt.Errorf("recover WAL failed: %w", err)
	}
	return store, nil
}

type AzureStore struct {
	client    *aztables.Client
	cache     *cache.ExpiringCache
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func (s *AzureStore) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	query := buildQuery(
		lexkey.EncodeFirst(api.INVENTORY, api.SPACES).ToHexString(),
		lexkey.EncodeLast(api.INVENTORY, api.SPACES).ToHexString(),
	)

	entities := NewAzureTableEnumerator(ctx, s.client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &query,
		Format: ptr(aztables.MetadataFormatNone),
	}))

	return enumerators.Map(entities, func(e *entity) (string, error) {
		return e.RowKey, nil
	})
}

func (s *AzureStore) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	ts := timestamp.GetTimestamp()
	bounds := calculateTimeBounds(ts, args.MinTimestamp, args.MaxTimestamp)

	query := buildQuery(
		getSpaceLowerBound(args.Space, bounds.Min, args.Offset).ToHexString(),
		lexkey.EncodeLast(api.DATA, api.SPACES, args.Space).ToHexString(),
	)

	return s.queryEntries(ctx, query, bounds.Min, bounds.Max)
}

func (s *AzureStore) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	query := buildQuery(
		lexkey.EncodeFirst(api.INVENTORY, api.SEGMENTS, space).ToHexString(),
		lexkey.EncodeLast(api.INVENTORY, api.SEGMENTS, space).ToHexString(),
	)

	entities := NewAzureTableEnumerator(ctx, s.client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &query,
		Format: ptr(aztables.MetadataFormatNone),
	}))

	return enumerators.Map(entities, func(e *entity) (string, error) {
		return e.RowKey, nil
	})
}

func (s *AzureStore) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	ts := timestamp.GetTimestamp()
	bounds := calculateSegmentBounds(ts, args)

	pLower := lexkey.EncodeFirst(api.DATA, api.SEGMENTS, args.Space, args.Segment).ToHexString()
	pUpper := lexkey.EncodeLast(api.DATA, api.SEGMENTS, args.Space, args.Segment).ToHexString()
	rLower := lexkey.EncodeFirst(bounds.MinSeq).ToHexString()
	rUpper := lexkey.EncodeLast(bounds.MaxSeq).ToHexString()

	query := fmt.Sprintf("PartitionKey ge '%s' and PartitionKey le '%s' and RowKey ge '%s' and RowKey le '%s'",
		pLower, pUpper, rLower, rUpper)

	entities := NewAzureTableEnumerator(ctx, s.client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &query,
		Format: ptr(aztables.MetadataFormatNone),
	}))

	entries := enumerators.Map(entities, func(e *entity) (*api.Entry, error) {
		return decodeEntry(e.Value)
	})

	return enumerators.TakeWhile(entries, func(e *api.Entry) bool {
		return e.Sequence > bounds.MinSeq &&
			e.Sequence <= bounds.MaxSeq &&
			e.Timestamp > bounds.MinTS &&
			e.Timestamp <= bounds.MaxTS
	})
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

	resp, err := s.client.GetEntity(ctx, pk, rk, nil)
	if err != nil {
		if isNotFoundError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", ErrPeekFailed, err)
	}

	entry, err := decodeSnappyEntryEntity(resp.Value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ErrPeekFailed, err)
	}
	s.cache.Set(cacheKey, entry)
	return entry, nil
}

func (s *AzureStore) Produce(ctx context.Context, args *api.Produce, records enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	if args == nil || args.Space == "" || args.Segment == "" {
		return enumerators.Error[*api.SegmentStatus](errors.New(ErrInvalidProduceArgs))
	}

	lastEntry, err := s.Peek(ctx, args.Space, args.Segment)
	if err != nil {
		return enumerators.Error[*api.SegmentStatus](fmt.Errorf("%s: %w", ErrPeekFailed, err))
	}
	if lastEntry == nil {
		lastEntry = &api.Entry{Sequence: 0, TRX: api.TRX{Number: 0}}
	}

	chunks := enumerators.ChunkByCount(records, BatchSize)
	var lastSeq, lastTrx = lastEntry.Sequence, lastEntry.TRX.Number

	return enumerators.Map(chunks, func(chunk enumerators.Enumerator[*api.Record]) (*api.SegmentStatus, error) {
		return s.processChunkWithRetry(ctx, args.Space, args.Segment, chunk, &lastSeq, &lastTrx)
	})
}

func (s *AzureStore) Close() {
	s.closeOnce.Do(func() {
		if !s.waitForTasks(ShutdownTimeout) {
			slog.Warn(LogWarnTimeoutTasks)
		}
		s.cache.Close()
	})
}

// Private Instance Methods

func (s *AzureStore) processChunkWithRetry(ctx context.Context, space, segment string, chunk enumerators.Enumerator[*api.Record], lastSeq, lastTrx *uint64) (*api.SegmentStatus, error) {
	var lastErr error
	for attempt := 0; attempt < MaxRetryAttempts; attempt++ {
		status, err := s.processChunk(ctx, space, segment, chunk, *lastSeq, *lastTrx)
		if err == nil {
			*lastSeq = status.LastSequence
			*lastTrx += 1
			return status, nil
		}
		if !isRetryableError(err) {
			return nil, err
		}
		lastErr = err
		time.Sleep(InitialRetryDelay * time.Duration(attempt+1)) // Changed from bit shift to multiplication
	}
	return nil, fmt.Errorf("failed after %d attempts: %w", MaxRetryAttempts, lastErr)
}

func (s *AzureStore) processChunk(ctx context.Context, space, segment string, chunk enumerators.Enumerator[*api.Record], lastSeq, lastTrx uint64) (*api.SegmentStatus, error) {
	s.wg.Add(1)
	defer s.wg.Done()

	trx := api.TRX{ID: uuid.New(), Number: lastTrx + 1}
	entries, err := createEntries(chunk, space, segment, trx, lastSeq)
	if err != nil {
		return nil, err
	}
	transaction := createTransaction(trx, space, segment, entries)
	if err := s.executeTransaction(ctx, transaction); err != nil {
		return nil, err
	}

	status := createSegmentStatus(space, segment, entries)

	return status, nil
}

func (s *AzureStore) recoverWAL(ctx context.Context) error {
	lower, upper := lexkey.EncodeFirst(api.TRANSACTION).ToHexString(), lexkey.EncodeLast(api.TRANSACTION).ToHexString()
	query := fmt.Sprintf("PartitionKey ge '%s' and PartitionKey le '%s'", lower, upper)

	pager := s.client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &query,
		Format: ptr(aztables.MetadataFormatNone),
	})
	transactions := enumerators.Map(
		NewAzureTableEnumerator(ctx, pager),
		func(e *entity) (*txn.Transaction, error) {
			transaction := &txn.Transaction{}
			if err := json.Unmarshal(e.Value, transaction); err != nil {
				return nil, fmt.Errorf("%s: %w", ErrUnmarshalTransaction, err)
			}
			if err := s.fanoutTransaction(ctx, transaction); err != nil {
				slog.Error(LogErrorFanout, "error", err)
				return nil, err
			}
			if err := s.cleanupWAL(ctx, e.PartitionKey, e.RowKey); err != nil {
				slog.Error(LogErrorWALCleanup, "error", err)
				return nil, err
			}
			return transaction, nil
		})

	return enumerators.Consume(transactions)
}

func (s *AzureStore) executeTransaction(ctx context.Context, transaction *txn.Transaction) error {
	transactionEntity, err := createTransactionEntity(transaction)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrTransactionCreate, err)
	}

	if _, err := s.client.AddEntity(ctx, mustMarshal(transactionEntity), nil); err != nil {
		return fmt.Errorf("%s: %w", ErrTransactionWrite, err)
	}

	if err := s.fanoutTransaction(ctx, transaction); err != nil {
		return fmt.Errorf("%s: %w", ErrTransactionFanout, err)
	}
	if err := s.cleanupWAL(ctx, transactionEntity.PartitionKey, transactionEntity.RowKey); err != nil {
		return fmt.Errorf("%s: %w", ErrWALCleanup, err)
	}

	return s.updateInventory(ctx, transaction.Space, transaction.Segment)
}

func (s *AzureStore) cleanupWAL(ctx context.Context, pk, rk string) error {
	if _, err := s.client.DeleteEntity(ctx, pk, rk, nil); err != nil {
		return fmt.Errorf("%s: %w", ErrWALCleanup, err)
	}
	return nil
}

func (s *AzureStore) fanoutTransaction(ctx context.Context, transaction *txn.Transaction) error {
	batch, err := prepareBatchEntries(transaction.Entries)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrBatchPrepare, err)
	}

	errChan := make(chan error, 3)
	var wg sync.WaitGroup
	wg.Add(3)

	go s.writeLastEntry(ctx, batch[len(batch)-1], errChan, &wg)
	go s.writeSegmentBatch(ctx, batch, errChan, &wg)
	go s.writeSpaceBatch(ctx, batch, errChan, &wg)

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AzureStore) writeLastEntry(ctx context.Context, entry batchEntry, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	entity := entity{
		PartitionKey: lexkey.Encode(LAST_ENTRY, entry.Entry.Space, entry.Entry.Segment).ToHexString(),
		RowKey:       lexkey.Encode(lexkey.EndMarker).ToHexString(),
		Value:        entry.EncodedValue,
	}

	if _, err := s.client.UpsertEntity(ctx, mustMarshal(entity), &aztables.UpsertEntityOptions{
		UpdateMode: aztables.UpdateModeReplace,
	}); err != nil {
		errChan <- fmt.Errorf("%s: %w", ErrBatchWrite, err)
	}
}

func (s *AzureStore) writeSegmentBatch(ctx context.Context, entries []batchEntry, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	entities := make([]entity, len(entries))
	for i, entry := range entries {
		entities[i] = entity{
			PartitionKey: lexkey.Encode(api.DATA, api.SEGMENTS, entry.Entry.Space, entry.Entry.Segment, entry.Entry.TRX.Number).ToHexString(),
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

	entities := make([]entity, len(entries))
	for i, entry := range entries {
		entities[i] = entity{
			PartitionKey: lexkey.Encode(api.DATA, api.SPACES, entry.Entry.Space, entry.Entry.Timestamp, entry.Entry.Segment).ToHexString(),
			RowKey:       lexkey.Encode(entry.Entry.Sequence).ToHexString(),
			Value:        entry.EncodedValue,
		}
	}

	if err := s.writeBatch(ctx, entities); err != nil {
		errChan <- err
	}
}

func (s *AzureStore) writeBatch(ctx context.Context, entities []entity) error {
	if len(entities) == 0 {
		return nil
	}

	actions := make([]aztables.TransactionAction, len(entities))
	for i := range entities {
		actions[i] = aztables.TransactionAction{
			ActionType: aztables.TransactionTypeInsertReplace,
			Entity:     mustMarshal(entities[i]),
		}
	}

	_, err := s.client.SubmitTransaction(ctx, actions, nil)
	if err != nil && err.Error() != "unexpected EOF" {
		return fmt.Errorf("%s: %w", ErrBatchWrite, err)
	}
	return nil
}

func (s *AzureStore) updateInventory(ctx context.Context, space, segment string) error {
	segmentKey := lexkey.Encode(api.INVENTORY, api.SEGMENTS, space, segment).ToHexString()

	_, ok := s.cache.Get(segmentKey)
	if ok {
		return nil
	}

	updateOptions := &aztables.UpsertEntityOptions{UpdateMode: aztables.UpdateModeReplace}

	if _, err := s.client.UpsertEntity(ctx, mustMarshal(entity{
		PartitionKey: segmentKey,
		RowKey:       segment,
	}), updateOptions); err != nil {
		return fmt.Errorf("%s: %w", ErrSegmentInventory, err)
	}

	spaceKey := lexkey.Encode(api.INVENTORY, api.SPACES, space).ToHexString()

	if _, err := s.client.UpsertEntity(ctx, mustMarshal(entity{
		PartitionKey: spaceKey,
		RowKey:       space,
	}), updateOptions); err != nil {
		return fmt.Errorf("%s: %w", ErrSpaceInventory, err)
	}

	s.cache.Set(segmentKey, struct{}{})
	return nil
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
	_, err := s.client.CreateTable(ctx, &aztables.CreateTableOptions{})
	if err == nil {
		return nil
	}

	var responseErr *azcore.ResponseError
	if errors.As(err, &responseErr) && responseErr.ErrorCode == string(aztables.TableAlreadyExists) {
		return nil
	}

	return fmt.Errorf("%s: %w", ErrTableCreation, err)
}

func (s *AzureStore) queryEntries(ctx context.Context, filter string, minTS, maxTS int64) enumerators.Enumerator[*api.Entry] {
	pager := s.client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
		Format: ptr(aztables.MetadataFormatNone),
	})

	entities := NewAzureTableEnumerator(ctx, pager)
	entries := enumerators.Map(entities, func(e *entity) (*api.Entry, error) {
		return decodeEntry(e.Value)
	})

	return enumerators.TakeWhile(entries, func(e *api.Entry) bool {
		return e.Timestamp > minTS && e.Timestamp <= maxTS
	})
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

func createTransactionEntity(transaction *txn.Transaction) (*entity, error) {
	value, err := codec.EncodeTransactionSnappy(transaction)
	if err != nil {
		return nil, err
	}

	return &entity{
		PartitionKey: lexkey.Encode(api.TRANSACTION, transaction.Space, transaction.Segment, transaction.TRX.Number).ToHexString(),
		RowKey:       lexkey.Encode(lexkey.EndMarker).ToHexString(),
		Value:        value,
	}, nil
}

func createEntries(chunk enumerators.Enumerator[*api.Record], space, segment string, trx api.TRX, lastSeq uint64) ([]*api.Entry, error) {
	ts := timestamp.GetTimestamp()
	enumerator := enumerators.Map(chunk, func(r *api.Record) (*api.Entry, error) {
		lastSeq++
		if r.Sequence != lastSeq {
			return nil, api.ERR_SEQUENCE_MISMATCH
		}
		return &api.Entry{
			TRX:       trx,
			Space:     space,
			Segment:   segment,
			Sequence:  r.Sequence,
			Timestamp: ts,
			Payload:   r.Payload,
			Metadata:  r.Metadata,
		}, nil
	})
	return enumerators.ToSlice(enumerator)
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
	return err != nil && strings.Contains(err.Error(), "ResourceNotFound")
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "Conflict") ||
		strings.Contains(errStr, "PreconditionFailed") ||
		strings.Contains(errStr, "ServiceUnavailable") ||
		strings.Contains(errStr, "429")
}

func decodeSnappyEntryEntity(value []byte) (*api.Entry, error) {
	var entity entity
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

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal: %v", err))
	}
	return data
}

func buildQuery(lower, upper string) string {
	return fmt.Sprintf("PartitionKey ge '%s' and PartitionKey le '%s'", lower, upper)
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

func getSpaceLowerBound(space string, minTS int64, offset lexkey.LexKey) lexkey.LexKey {
	if len(offset) > 0 {
		return offset
	}
	return lexkey.EncodeFirst(api.DATA, api.SPACES, space, minTS)
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
	}
	return bounds
}

func ptr[T any](v T) *T {
	return &v
}
