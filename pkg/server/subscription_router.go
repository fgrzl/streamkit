package server

import (
	"context"
	"log/slog"
	"sync"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/bus"
	"github.com/google/uuid"
)

func (n *defaultNode) registerSubscriptionTarget(ctx context.Context, space, segment string, bidi api.BidiStream) (*spaceSubscriptionTarget, error) {
	n.subscriptionRoutersMu.Lock()
	defer n.subscriptionRoutersMu.Unlock()

	router, ok := n.subscriptionRouters[space]
	if !ok {
		messageBus, err := n.busFactory.Get(ctx)
		if err != nil {
			return nil, err
		}

		router = &spaceSubscriptionRouter{
			node:                n,
			space:               space,
			wildcardSubscribers: make(map[string]*spaceSubscriptionTarget),
			segmentSubscribers:  make(map[string]map[string]*spaceSubscriptionTarget),
		}
		route := api.GetSegmentNotificationRoute(n.storeID, space)
		subscription, err := bus.Subscribe(messageBus, route, router.handleNotification)
		if err != nil {
			return nil, err
		}
		router.busSubscription = subscription
		n.subscriptionRouters[space] = router
	}

	target := &spaceSubscriptionTarget{
		id:              uuid.NewString(),
		router:          router,
		segment:         segment,
		bidi:            bidi,
		snapshotPending: true,
		bufferedStatus:  make(map[string]*api.SegmentStatus),
	}

	router.mu.Lock()
	if target.segment == "*" {
		router.wildcardSubscribers[target.id] = target
	} else {
		segmentSubscribers := router.segmentSubscribers[target.segment]
		if segmentSubscribers == nil {
			segmentSubscribers = make(map[string]*spaceSubscriptionTarget)
			router.segmentSubscribers[target.segment] = segmentSubscribers
		}
		segmentSubscribers[target.id] = target
	}
	router.subscriberCount++
	router.mu.Unlock()

	return target, nil
}

type spaceSubscriptionRouter struct {
	node                *defaultNode
	space               string
	mu                  sync.RWMutex
	wildcardSubscribers map[string]*spaceSubscriptionTarget
	segmentSubscribers  map[string]map[string]*spaceSubscriptionTarget
	subscriberCount     int
	busSubscription     bus.Subscription
	closeOnce           sync.Once
}

func (r *spaceSubscriptionRouter) unregisterSubscriber(target *spaceSubscriptionTarget) {
	if r == nil || target == nil || target.id == "" {
		return
	}

	var subscription bus.Subscription
	r.node.subscriptionRoutersMu.Lock()
	r.mu.Lock()
	removed := false
	if target.segment == "*" {
		if _, exists := r.wildcardSubscribers[target.id]; exists {
			delete(r.wildcardSubscribers, target.id)
			removed = true
		}
	} else if segmentSubscribers := r.segmentSubscribers[target.segment]; segmentSubscribers != nil {
		if _, exists := segmentSubscribers[target.id]; exists {
			delete(segmentSubscribers, target.id)
			removed = true
			if len(segmentSubscribers) == 0 {
				delete(r.segmentSubscribers, target.segment)
			}
		}
	}
	if removed && r.subscriberCount > 0 {
		r.subscriberCount--
	}
	if r.subscriberCount == 0 && r.node.subscriptionRouters[r.space] == r {
		delete(r.node.subscriptionRouters, r.space)
		subscription = r.busSubscription
		r.busSubscription = nil
	}
	r.mu.Unlock()
	r.node.subscriptionRoutersMu.Unlock()

	if subscription != nil {
		_ = subscription.Unsubscribe()
	}
}

func (r *spaceSubscriptionRouter) close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		subscription := r.busSubscription
		r.busSubscription = nil
		r.wildcardSubscribers = make(map[string]*spaceSubscriptionTarget)
		r.segmentSubscribers = make(map[string]map[string]*spaceSubscriptionTarget)
		r.subscriberCount = 0
		r.mu.Unlock()

		if subscription != nil {
			_ = subscription.Unsubscribe()
		}
	})
}

func (r *spaceSubscriptionRouter) handleNotification(ctx context.Context, msg *api.SegmentNotification) error {
	if msg == nil || msg.SegmentStatus == nil {
		return nil
	}

	targets := r.matchingSubscribers(msg.SegmentStatus)

	for _, target := range targets {
		if err := target.acceptNotification(msg.SegmentStatus); err != nil {
			slog.WarnContext(ctx, "server: failed to deliver subscription notification",
				logContextFields(ctx, r.node.storeID,
					slog.String("space", msg.SegmentStatus.Space),
					slog.String("segment", msg.SegmentStatus.Segment),
					"err", err)...)
			r.unregisterSubscriber(target)
			target.bidi.Close(err)
		}
	}

	return nil
}

func (r *spaceSubscriptionRouter) matchingSubscribers(status *api.SegmentStatus) []*spaceSubscriptionTarget {
	if r == nil || status == nil {
		return nil
	}

	r.mu.RLock()
	segmentSubscribers := r.segmentSubscribers[status.Segment]
	targets := make([]*spaceSubscriptionTarget, 0, len(r.wildcardSubscribers)+len(segmentSubscribers))
	for _, target := range r.wildcardSubscribers {
		targets = append(targets, target)
	}
	for _, target := range segmentSubscribers {
		targets = append(targets, target)
	}
	r.mu.RUnlock()

	return targets
}

type spaceSubscriptionTarget struct {
	id              string
	router          *spaceSubscriptionRouter
	segment         string
	bidi            api.BidiStream
	mu              sync.Mutex
	snapshotPending bool
	bufferedStatus  map[string]*api.SegmentStatus
}

func (t *spaceSubscriptionTarget) matches(status *api.SegmentStatus) bool {
	if t == nil || status == nil {
		return false
	}
	return t.segment == "*" || t.segment == status.Segment
}

func (t *spaceSubscriptionTarget) acceptNotification(status *api.SegmentStatus) error {
	if !t.matches(status) {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.snapshotPending {
		t.bufferedStatus[status.Segment] = cloneSegmentStatus(status)
		return nil
	}
	return t.bidi.Encode(status)
}

func (t *spaceSubscriptionTarget) emitSnapshot(statuses []*api.SegmentStatus) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, status := range statuses {
		if !t.matches(status) {
			continue
		}
		if err := t.bidi.Encode(status); err != nil {
			return err
		}
	}

	for _, status := range sortedBufferedStatuses(t.bufferedStatus) {
		if err := t.bidi.Encode(status); err != nil {
			return err
		}
	}

	t.snapshotPending = false
	t.bufferedStatus = nil
	return nil
}
