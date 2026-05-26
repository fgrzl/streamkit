package server

import (
	"fmt"
	"sort"
	"testing"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
)

type nopEncodeBidi struct {
	closed chan struct{}
}

func newNopEncodeBidi() *nopEncodeBidi {
	return &nopEncodeBidi{closed: make(chan struct{})}
}

func (b *nopEncodeBidi) Encode(any) error { return nil }
func (b *nopEncodeBidi) Decode(any) error {
	<-b.closed
	return nil
}
func (b *nopEncodeBidi) CloseSend(error) error { return nil }
func (b *nopEncodeBidi) Close(error)           {}
func (b *nopEncodeBidi) EndOfStreamError() error {
	return nil
}
func (b *nopEncodeBidi) Closed() <-chan struct{} { return b.closed }

func benchWorkerMetadata() map[string]string {
	return map[string]string{
		"role":    "processor",
		"region":  "us-east",
		"version": "1.2.3",
	}
}

func benchWorkerID(i int) uuid.UUID {
	return uuid.MustParse(fmt.Sprintf("00000000-0000-4000-8000-%012x", i))
}

func newBenchWorkerRouter(workerCount int) *workerPresenceRouter {
	storeID := uuid.New()
	router := &workerPresenceRouter{
		node:        &defaultNode{storeID: storeID},
		workers:     make(map[uuid.UUID]*workerEntry, workerCount),
		workerIDs:   make([]uuid.UUID, 0, workerCount),
		subscribers: make(map[string]*inventorySubscriberTarget),
	}
	now := int64(1_700_000_000_000)
	metadata := benchWorkerMetadata()
	for i := 0; i < workerCount; i++ {
		id := benchWorkerID(i)
		router.workers[id] = &workerEntry{
			workerID:   id,
			metadata:   metadata,
			lastSeenAt: now,
		}
		router.workerIDs = append(router.workerIDs, id)
	}
	sort.Slice(router.workerIDs, func(i, j int) bool {
		return api.CompareWorkerID(router.workerIDs[i], router.workerIDs[j]) < 0
	})
	return router
}

func addBenchSubscribers(router *workerPresenceRouter, count int) {
	for i := 0; i < count; i++ {
		target := newInventorySubscriberTarget(router, newNopEncodeBidi())
		router.subscribers[target.id] = target
		target.start()
	}
	router.subscriberCount = count
}

func BenchmarkBuildInventory(b *testing.B) {
	for _, workers := range []int{1, 10, 50, 100, 500} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			router := newBenchWorkerRouter(workers)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				router.mu.RLock()
				_ = router.buildInventoryLocked(1_700_000_000_000)
				router.mu.RUnlock()
			}
		})
	}
}

func BenchmarkPublishInventoryLocal(b *testing.B) {
	for _, workers := range []int{10, 100} {
		for _, subs := range []int{1, 10, 50} {
			b.Run(fmt.Sprintf("workers=%d/subs=%d", workers, subs), func(b *testing.B) {
				router := newBenchWorkerRouter(workers)
				addBenchSubscribers(router, subs)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					router.publishInventory()
				}
			})
		}
	}
}

func BenchmarkRegisterWorker(b *testing.B) {
	for _, subs := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			router := newBenchWorkerRouter(10)
			addBenchSubscribers(router, subs)
			subscriber := newInventorySubscriberTarget(router, newNopEncodeBidi())
			subscriber.start()
			metadata := benchWorkerMetadata()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = router.registerWorker(benchWorkerID(i%64), metadata, subscriber, 30)
			}
		})
	}
}

func BenchmarkAcceptInventoryCoalesce(b *testing.B) {
	router := &workerPresenceRouter{
		node:        &defaultNode{storeID: uuid.New()},
		workers:     make(map[uuid.UUID]*workerEntry),
		subscribers: make(map[string]*inventorySubscriberTarget),
	}
	target := newInventorySubscriberTarget(router, &slowEncodeBidi{})
	target.start()
	inv := &api.WorkerInventory{
		Workers: make([]api.WorkerInfo, 100),
	}
	for i := range inv.Workers {
		inv.Workers[i] = api.WorkerInfo{WorkerID: benchWorkerID(i)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		target.acceptInventory(inv)
	}
}
