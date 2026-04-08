package client

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newBenchmarkDispatchClient(handlerTimeout time.Duration, maxConcurrentHandlers int) *client {
	c := &client{
		handlerTimeout:        handlerTimeout,
		maxConcurrentHandlers: maxConcurrentHandlers,
		subscriptionShards:    newSubscriptionRegistryShards(),
		produceLocks:          make(map[string]*sync.Mutex),
	}
	c.startHandlerWorkers()
	return c
}

func benchmarkSubscriptionDispatch(b *testing.B, handlerTimeout time.Duration, pendingPerSegment bool, statuses []SegmentStatus) {
	maxConcurrentHandlers := len(statuses)
	if maxConcurrentHandlers < 1 {
		maxConcurrentHandlers = 1
	}

	c := newBenchmarkDispatchClient(handlerTimeout, maxConcurrentHandlers)
	b.Cleanup(func() {
		_ = c.Close()
	})

	sub := &activeSubscription{
		id:                "bench-sub",
		ctx:               context.Background(),
		mailboxCh:         make(chan struct{}, 1),
		pendingPerSegment: pendingPerSegment,
	}
	handler := func(*SegmentStatus) {}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for j, status := range statuses {
			status.LastSequence = uint64(i*len(statuses) + j + 1)
			sub.queuePendingStatus(status)
		}
		c.drainPendingSubscriptionStatuses(ctx, sub, sub.id, handler, &wg)
		wg.Wait()
		select {
		case <-sub.mailboxCh:
		default:
		}
	}
}

func BenchmarkPendingStatusExactCoalesced(b *testing.B) {
	sub := &activeSubscription{}
	status := SegmentStatus{Space: "space", Segment: "segment", LastSequence: 1}
	sub.queuePendingStatus(status)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status.LastSequence = uint64(i + 2)
		sub.queuePendingStatus(status)
	}
}

func BenchmarkPendingStatusWildcardCoalesced(b *testing.B) {
	sub := &activeSubscription{pendingPerSegment: true}
	status := SegmentStatus{Space: "space", Segment: "segment", LastSequence: 1}
	sub.queuePendingStatus(status)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status.LastSequence = uint64(i + 2)
		sub.queuePendingStatus(status)
	}
}

func BenchmarkPendingStatusWildcardDistinctSegments(b *testing.B) {
	segments := [...]string{"seg-a", "seg-b", "seg-c", "seg-d", "seg-e", "seg-f", "seg-g", "seg-h"}
	sub := &activeSubscription{pendingPerSegment: true}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status := SegmentStatus{
			Space:        "space",
			Segment:      segments[i%len(segments)],
			LastSequence: uint64(i + 1),
		}
		sub.queuePendingStatus(status)
		if len(sub.pendingOrder) == len(segments) {
			for {
				if _, ok := sub.takePendingStatus(); !ok {
					break
				}
			}
		}
	}
}

func BenchmarkSubscriptionDispatchExact(b *testing.B) {
	statuses := []SegmentStatus{{Space: "space", Segment: "segment"}}

	b.Run("timeout_enabled", func(b *testing.B) {
		benchmarkSubscriptionDispatch(b, 50*time.Millisecond, false, statuses)
	})

	b.Run("timeout_disabled", func(b *testing.B) {
		benchmarkSubscriptionDispatch(b, 0, false, statuses)
	})
}

func BenchmarkSubscriptionDispatchWildcardDistinctSegments(b *testing.B) {
	statuses := []SegmentStatus{
		{Space: "space", Segment: "seg-a"},
		{Space: "space", Segment: "seg-b"},
		{Space: "space", Segment: "seg-c"},
		{Space: "space", Segment: "seg-d"},
		{Space: "space", Segment: "seg-e"},
		{Space: "space", Segment: "seg-f"},
		{Space: "space", Segment: "seg-g"},
		{Space: "space", Segment: "seg-h"},
	}

	b.Run("timeout_enabled", func(b *testing.B) {
		benchmarkSubscriptionDispatch(b, 50*time.Millisecond, true, statuses)
	})

	b.Run("timeout_disabled", func(b *testing.B) {
		benchmarkSubscriptionDispatch(b, 0, true, statuses)
	})
}
