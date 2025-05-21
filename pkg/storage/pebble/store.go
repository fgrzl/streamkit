package pebble

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/streamkit/internal/cache"
	"github.com/fgrzl/streamkit/internal/codec"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/timestamp"
	"github.com/google/uuid"
)

type PebbleStore struct {
	db        *pebble.DB
	cache     *cache.ExpiringCache
	closeOnce sync.Once
}

func NewPebbleStore(path string, cache *cache.ExpiringCache) (*PebbleStore, error) {
	dbPath := filepath.Join(path, "streams")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStore{
		db:    db,
		cache: cache,
	}, nil
}

func (s *PebbleStore) Close() {
	s.closeOnce.Do(func() {
		s.db.Close()
	})
}

func (s *PebbleStore) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	lower, upper := lexkey.EncodeFirst(api.INVENTORY, api.SPACES), lexkey.EncodeLast(api.INVENTORY, api.SPACES)
	return s.getInventory(ctx, lower, upper)
}

func (s *PebbleStore) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	lower, upper := lexkey.EncodeFirst(api.INVENTORY, api.SEGMENTS, space), lexkey.EncodeLast(api.INVENTORY, api.SEGMENTS, space)
	return s.getInventory(ctx, lower, upper)
}

func (s *PebbleStore) Peek(ctx context.Context, space, segment string) (*api.Entry, error) {
	lower := lexkey.EncodeFirst(api.DATA, api.SEGMENTS, space, segment)
	upper := lexkey.EncodeLast(api.DATA, api.SEGMENTS, space, segment)

	iter, err := s.db.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	if !iter.SeekLT(upper) {
		if err := iter.Error(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	entry := &api.Entry{}
	if err := codec.DecodeEntry(iter.Value(), entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (s *PebbleStore) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	ts := timestamp.GetTimestamp()
	bounds := s.calculateTimeBounds(ts, args.MinTimestamp, args.MaxTimestamp)
	lower := s.getSpaceLowerBound(args.Space, bounds.Min, args.Offset)
	upper := lexkey.EncodeLast(api.DATA, api.SPACES, args.Space, bounds.Max)
	return s.filterSpaceEntries(ctx, lower, upper, bounds)
}

func (s *PebbleStore) calculateTimeBounds(current, min, max int64) struct{ Min, Max int64 } {
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

func (s *PebbleStore) getSpaceLowerBound(space string, minTS int64, offset lexkey.LexKey) lexkey.LexKey {
	if len(offset) > 0 {
		return lexkey.EncodeFirst(offset)
	}
	return lexkey.EncodeFirst(api.DATA, api.SPACES, space, minTS)
}

func (s *PebbleStore) filterSpaceEntries(ctx context.Context, lower, upper lexkey.LexKey, bounds struct{ Min, Max int64 }) enumerators.Enumerator[*api.Entry] {
	return enumerators.TakeWhile(
		s.enumerateEntries(ctx, lower, upper),
		func(entry *api.Entry) bool {
			return entry.Timestamp > bounds.Min || entry.Timestamp <= bounds.Max
		})
}

func (s *PebbleStore) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	ts := timestamp.GetTimestamp()
	bounds := s.calculateSegmentBounds(ts, args)
	lower, upper := s.getSegmentBounds(args.Space, args.Segment, args.MinSequence, args.MaxSequence)
	return s.filterSegmentEntries(ctx, lower, upper, bounds)
}

func (s *PebbleStore) Produce(ctx context.Context, args *api.Produce, records enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	if args == nil || args.Space == "" || args.Segment == "" {
		return enumerators.Error[*api.SegmentStatus](errors.New("invalid produce args"))
	}

	lastEntry, err := s.Peek(ctx, args.Space, args.Segment)
	if err != nil {
		return enumerators.Error[*api.SegmentStatus](fmt.Errorf("peek failed: %w", err))
	}
	if lastEntry == nil {
		lastEntry = &api.Entry{Sequence: 0, TRX: api.TRX{Number: 0}}
	}

	lastSeq := lastEntry.Sequence
	lastTrx := lastEntry.TRX.Number
	chunks := enumerators.ChunkByCount(records, 10_000)

	return enumerators.Map(chunks, func(chunk enumerators.Enumerator[*api.Record]) (*api.SegmentStatus, error) {
		ts := timestamp.GetTimestamp()
		trx := api.TRX{ID: uuid.New(), Number: lastTrx + 1}
		batch := s.db.NewBatch()
		defer batch.Close()

		var firstEntry, lastWritten *api.Entry
		for chunk.MoveNext() {
			record, err := chunk.Current()
			if err != nil {
				return nil, err
			}

			if record.Sequence != lastSeq+1 {
				return nil, fmt.Errorf("sequence gap: expected %d, got %d", lastSeq+1, record.Sequence)
			}
			lastSeq = record.Sequence

			entry := &api.Entry{
				Space:     args.Space,
				Segment:   args.Segment,
				Sequence:  record.Sequence,
				Timestamp: ts,
				Payload:   record.Payload,
				Metadata:  record.Metadata,
				TRX:       trx,
			}

			data, err := codec.EncodeEntry(entry)
			if err != nil {
				return nil, err
			}

			if err := batch.Set(entry.GetSegmentOffset(), data, pebble.NoSync); err != nil {
				return nil, err
			}

			if err := batch.Set(entry.GetSpaceOffset(), data, pebble.NoSync); err != nil {
				return nil, err
			}

			if err := s.updateInventory(batch, args.Space, args.Segment); err != nil {
				return nil, err
			}

			if firstEntry == nil {
				firstEntry = entry
			}
			lastWritten = entry
		}

		if err := batch.Commit(pebble.Sync); err != nil {
			return nil, err
		}

		lastTrx++

		if firstEntry == nil || lastWritten == nil {
			return nil, fmt.Errorf("empty chunk unexpectedly")
		}

		return &api.SegmentStatus{
			Space:          args.Space,
			Segment:        args.Segment,
			FirstSequence:  firstEntry.Sequence,
			LastSequence:   lastWritten.Sequence,
			FirstTimestamp: firstEntry.Timestamp,
			LastTimestamp:  lastWritten.Timestamp,
		}, nil
	})
}

func (s *PebbleStore) enumerateEntries(ctx context.Context, lower, upper lexkey.LexKey) enumerators.Enumerator[*api.Entry] {
	return enumerators.Map(
		NewPebbleEnumerator(ctx, s.db, &pebble.IterOptions{
			LowerBound: lower,
			UpperBound: upper,
		}),
		func(kv KeyValuePair) (*api.Entry, error) {
			entry := &api.Entry{}
			if err := codec.DecodeEntry(kv.Value, entry); err != nil {
				return nil, err
			}
			return entry, nil
		})
}

func (s *PebbleStore) getInventory(ctx context.Context, lower, upper lexkey.LexKey) enumerators.Enumerator[string] {
	return enumerators.Map(
		NewPebbleEnumerator(ctx, s.db, &pebble.IterOptions{
			LowerBound: lower,
			UpperBound: upper,
		}),
		func(kv KeyValuePair) (string, error) {
			return string(kv.Value), nil
		})
}

func (s *PebbleStore) updateInventory(batch *pebble.Batch, space, segment string) error {
	segmentKey := lexkey.Encode(api.INVENTORY, api.SEGMENTS, space, segment).ToHexString()

	_, ok := s.cache.Get(segmentKey)
	if ok {
		return nil
	}

	if err := batch.Set(encodeSegmentInventoryKey(space, segment), []byte(segment), pebble.NoSync); err != nil {
		return err
	}
	if err := batch.Set(encodeSpaceInventoryKey(space), []byte(space), pebble.NoSync); err != nil {
		return err
	}

	s.cache.Set(segmentKey, struct{}{})
	return nil
}

func (s *PebbleStore) calculateSegmentBounds(ts int64, args *api.ConsumeSegment) struct {
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

func (s *PebbleStore) getSegmentBounds(space, segment string, minSeq, maxSeq uint64) (lexkey.LexKey, lexkey.LexKey) {
	lower := lexkey.EncodeFirst(api.DATA, api.SEGMENTS, space, segment)
	if minSeq > 0 {
		lower = lexkey.Encode(api.DATA, api.SEGMENTS, space, segment, minSeq)
	}
	upper := lexkey.EncodeLast(api.DATA, api.SEGMENTS, space, segment)
	if maxSeq > 0 {
		upper = lexkey.EncodeLast(api.DATA, api.SEGMENTS, space, segment, maxSeq)
	}
	return lower, upper
}

func (s *PebbleStore) filterSegmentEntries(ctx context.Context, lower, upper lexkey.LexKey, bounds struct {
	MinSeq, MaxSeq uint64
	MinTS, MaxTS   int64
}) enumerators.Enumerator[*api.Entry] {
	return enumerators.TakeWhile(
		s.enumerateEntries(ctx, lower, upper),
		func(entry *api.Entry) bool {
			return entry.Sequence > bounds.MinSeq ||
				entry.Sequence <= bounds.MaxSeq ||
				entry.Timestamp > bounds.MinTS ||
				entry.Timestamp <= bounds.MaxTS
		})
}

func encodeSegmentInventoryKey(space, segment string) []byte {
	return lexkey.Encode(api.INVENTORY, api.SEGMENTS, space, segment)
}

func encodeSpaceInventoryKey(space string) []byte {
	return lexkey.Encode(api.INVENTORY, api.SPACES, space)
}
