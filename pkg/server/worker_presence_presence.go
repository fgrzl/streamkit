package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
)

const workerPresenceSweeperInterval = 5 * time.Second

var errWorkerIDRegisteredElsewhere = fmt.Errorf("worker_id registered by another stream")

func workerPresenceRenewIntervalSeconds(heartbeatIntervalSeconds int64) int64 {
	return api.WorkerPresenceRenewIntervalSeconds(heartbeatIntervalSeconds)
}

func (r *workerPresenceRouter) ensurePresenceSweeper() {
	r.sweeperOnce.Do(func() {
		r.sweeperCtx, r.sweeperCancel = context.WithCancel(context.Background())
		go r.runPresenceSweeper(r.sweeperCtx)
	})
}

func (r *workerPresenceRouter) runPresenceSweeper(ctx context.Context) {
	ticker := time.NewTicker(workerPresenceSweeperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.evictStaleWorkers()
		}
	}
}

func (r *workerPresenceRouter) evictStaleWorkers() {
	if r == nil {
		return
	}

	now := time.Now()
	var staleTargets []*inventorySubscriberTarget

	r.mu.Lock()
	for i := 0; i < len(r.workerIDs); {
		workerID := r.workerIDs[i]
		entry := r.workers[workerID]
		if entry == nil {
			r.workerIDs = append(r.workerIDs[:i], r.workerIDs[i+1:]...)
			continue
		}

		ttl := api.WorkerPresenceTTL(entry.presenceIntervalSeconds)
		if now.Sub(time.UnixMilli(entry.lastSeenAt)) <= ttl {
			i++
			continue
		}

		if target := r.subscribers[entry.subscriberID]; target != nil {
			staleTargets = append(staleTargets, target)
		}
		delete(r.workers, workerID)
		r.workerIDs = append(r.workerIDs[:i], r.workerIDs[i+1:]...)
	}

	var inventory *api.WorkerInventory
	var targets []*inventorySubscriberTarget
	if len(staleTargets) > 0 {
		inventory, targets = r.snapshotForFanOutLocked(api.NowUnixMilli())
	}
	r.mu.Unlock()

	if inventory != nil {
		r.deliverInventory(inventory, targets)
	}

	for _, target := range staleTargets {
		slog.WarnContext(context.Background(), "server: evicted stale worker",
			logContextFields(context.Background(), r.node.storeID,
				slog.String("worker_id", target.workerID.String()))...)
		r.unregisterSubscriber(target)
		target.close(fmt.Errorf("worker presence expired"), true)
	}
}

func (r *workerPresenceRouter) renewWorker(workerID uuid.UUID, subscriberID string, seenAt int64) {
	if r == nil || workerID == uuid.Nil || subscriberID == "" {
		return
	}
	if seenAt <= 0 {
		seenAt = api.NowUnixMilli()
	}

	r.mu.Lock()
	entry := r.workers[workerID]
	if entry == nil || entry.subscriberID != subscriberID {
		r.mu.Unlock()
		return
	}
	entry.lastSeenAt = seenAt
	r.mu.Unlock()
}

func (n *defaultNode) readWorkerPresenceRenewals(
	ctx context.Context,
	workerID uuid.UUID,
	subscriber *inventorySubscriberTarget,
	bidi api.BidiStream,
	onFatal func(error),
) {
	if workerID == uuid.Nil || subscriber == nil || bidi == nil {
		return
	}

	fatal := func(err error) {
		if onFatal == nil || err == nil {
			return
		}
		onFatal(err)
	}

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		envelope := &polymorphic.Envelope{}
		err := bidi.Decode(envelope)
		if err != nil {
			if isBenignInitialEnvelopeDecodeErr(err) {
				return
			}
			fatal(err)
			return
		}
		if envelope.Content == nil {
			continue
		}

		presence, ok := envelope.Content.(*api.Presence)
		if !ok {
			err := fmt.Errorf("unexpected worker presence stream message type: %T", envelope.Content)
			slog.WarnContext(ctx, "server: unexpected worker presence stream message",
				logContextFields(ctx, n.storeID,
					slog.String("worker_id", workerID.String()),
					slog.String("type", fmt.Sprintf("%T", envelope.Content)))...)
			fatal(err)
			return
		}
		if presence.Heartbeat {
			continue
		}
		if presence.WorkerID != uuid.Nil && presence.WorkerID != workerID {
			err := fmt.Errorf("worker presence renew worker_id mismatch: expected %s got %s",
				workerID, presence.WorkerID)
			slog.WarnContext(ctx, "server: worker presence renew worker_id mismatch",
				logContextFields(ctx, n.storeID,
					slog.String("expected_worker_id", workerID.String()),
					slog.String("actual_worker_id", presence.WorkerID.String()))...)
			fatal(err)
			return
		}

		seenAt := presence.Timestamp
		if seenAt <= 0 {
			seenAt = api.NowUnixMilli()
		}
		subscriber.router.renewWorker(workerID, subscriber.id, seenAt)
	}
}
