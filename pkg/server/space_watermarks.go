package server

import (
	"context"
	"math"
	"sync"

	"github.com/fgrzl/timestamp"
)

type spaceWatermarkToken struct {
	space string
	id    uint64
}

type activeSpaceWrite struct {
	begin       int64
	committedAt int64 // last committed entry timestamp; 0 until the chunk is known
}

type spaceWatermarks struct {
	mu     sync.Mutex
	cond   *sync.Cond
	nextID uint64
	active map[string]map[uint64]activeSpaceWrite
}

func newSpaceWatermarks() *spaceWatermarks {
	w := &spaceWatermarks{
		active: make(map[string]map[uint64]activeSpaceWrite),
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *spaceWatermarks) Begin(space string) spaceWatermarkToken {
	begin := timestamp.GetTimestamp()

	w.mu.Lock()
	defer w.mu.Unlock()
	w.nextID++
	token := spaceWatermarkToken{
		space: space,
		id:    w.nextID,
	}
	byID := w.active[space]
	if byID == nil {
		byID = make(map[uint64]activeSpaceWrite)
		w.active[space] = byID
	}
	byID[token.id] = activeSpaceWrite{begin: begin}
	w.cond.Broadcast()
	return token
}

func (w *spaceWatermarks) NoteCommitted(token spaceWatermarkToken, entryTimestamp int64) {
	if entryTimestamp <= 0 {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	byID := w.active[token.space]
	if byID == nil {
		return
	}
	write, ok := byID[token.id]
	if !ok {
		return
	}
	if write.committedAt == 0 || entryTimestamp < write.committedAt {
		write.committedAt = entryTimestamp
	}
	byID[token.id] = write
	w.cond.Broadcast()
}

func (w *spaceWatermarks) End(token spaceWatermarkToken) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if byID := w.active[token.space]; byID != nil {
		delete(byID, token.id)
		if len(byID) == 0 {
			delete(w.active, token.space)
		}
	}
	w.cond.Broadcast()
}

func (w *spaceWatermarks) SafeMaxTimestamp(space string) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.safeMaxTimestampLocked(space, 0)
}

func (w *spaceWatermarks) IsTimestampVisible(space string, ts int64) bool {
	if ts <= 0 {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return ts <= w.safeMaxTimestampLocked(space, 0)
}

func (w *spaceWatermarks) WaitUntilVisible(ctx context.Context, space string, ts int64) error {
	return w.waitUntilVisible(ctx, space, ts, 0)
}

func (w *spaceWatermarks) WaitUntilVisibleForPeers(ctx context.Context, space string, ts int64, token spaceWatermarkToken) error {
	return w.waitUntilVisible(ctx, space, ts, token.id)
}

func (w *spaceWatermarks) waitUntilVisible(ctx context.Context, space string, ts int64, beforeTokenID uint64) error {
	if ts <= 0 {
		return nil
	}
	stop := context.AfterFunc(ctx, func() {
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
	})
	defer stop()

	w.mu.Lock()
	defer w.mu.Unlock()
	for ts > w.safeMaxTimestampLocked(space, beforeTokenID) {
		if err := ctx.Err(); err != nil {
			return err
		}
		w.cond.Wait()
	}
	return ctx.Err()
}

// safeMaxTimestampLocked returns the newest timestamp visible to readers.
// When beforeTokenID is non-zero, only active writes started before that token
// (lower token id) are considered, so concurrent produce chunks do not block
// each other's notifications.
func (w *spaceWatermarks) safeMaxTimestampLocked(space string, beforeTokenID uint64) int64 {
	minFence := int64(math.MaxInt64)
	for id, write := range w.active[space] {
		if beforeTokenID != 0 && id >= beforeTokenID {
			continue
		}
		fence := write.fenceTimestamp()
		if fence < minFence {
			minFence = fence
		}
	}
	if minFence == math.MaxInt64 {
		return timestamp.GetTimestamp()
	}
	return minFence - 1
}

func (w activeSpaceWrite) fenceTimestamp() int64 {
	if w.committedAt == 0 {
		return w.begin
	}
	return min64(w.begin, w.committedAt)
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
