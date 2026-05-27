package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShouldSafeMaxUseBeginUntilEarlierCommittedTimestamp(t *testing.T) {
	w := newSpaceWatermarks()
	token := w.Begin("s")

	w.mu.Lock()
	begin := w.active["s"][token.id].begin
	w.mu.Unlock()

	require.Equal(t, begin-1, w.SafeMaxTimestamp("s"))

	w.NoteCommitted(token, begin+100)
	require.Equal(t, begin-1, w.SafeMaxTimestamp("s"))

	w.NoteCommitted(token, begin-5)
	require.Equal(t, begin-6, w.SafeMaxTimestamp("s"))
}

func TestShouldSafeMaxUseCommittedTimestampWhenEarlierThanBegin(t *testing.T) {
	w := newSpaceWatermarks()
	token := w.Begin("s")

	w.mu.Lock()
	begin := w.active["s"][token.id].begin
	w.mu.Unlock()

	w.NoteCommitted(token, begin-5)
	require.Equal(t, begin-6, w.SafeMaxTimestamp("s"))
}

func TestShouldWaitUntilVisibleUnblocksAfterActiveWriteEnds(t *testing.T) {
	w := newSpaceWatermarks()
	token := w.Begin("s")

	w.mu.Lock()
	begin := w.active["s"][token.id].begin
	w.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- w.WaitUntilVisible(context.Background(), "s", begin)
	}()

	require.Eventually(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return len(w.active["s"]) == 1
	}, time.Second, 10*time.Millisecond)

	w.End(token)
	require.NoError(t, <-done)
}

func TestShouldWaitUntilVisibleForPeersIgnoresConcurrentActiveWrite(t *testing.T) {
	w := newSpaceWatermarks()
	first := w.Begin("s")
	second := w.Begin("s")

	w.mu.Lock()
	target := w.active["s"][second.id].begin
	w.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- w.WaitUntilVisibleForPeers(context.Background(), "s", target, second)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return false
		default:
			return true
		}
	}, time.Second, 10*time.Millisecond)

	w.End(first)
	require.NoError(t, <-done)
	w.End(second)
}

func TestShouldIsTimestampVisibleReflectsActiveWrites(t *testing.T) {
	w := newSpaceWatermarks()
	token := w.Begin("s")

	w.mu.Lock()
	fence := w.active[token.space][token.id].fenceTimestamp()
	w.mu.Unlock()

	require.False(t, w.IsTimestampVisible("s", fence))
	require.True(t, w.IsTimestampVisible("s", fence-1))

	w.End(token)
	require.True(t, w.IsTimestampVisible("s", fence))
}
