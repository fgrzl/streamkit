package test

import (
	"context"
	"crypto/rand"
	"math/big"
	"sync"

	"github.com/fgrzl/streams/broker"
)

var _ broker.Bus = &mockBus{}

type mockBus struct {
	mu             sync.RWMutex
	handlers       map[string]map[*TestSubscription]broker.SubscriptionHandler
	streamHandlers map[string]map[*TestSubscription]broker.StreamSubscriptionHandler
}

// NewMockBus initializes the bus with an empty handler map.
func NewMockBus() broker.Bus {
	return &mockBus{
		handlers:       make(map[string]map[*TestSubscription]broker.SubscriptionHandler),
		streamHandlers: make(map[string]map[*TestSubscription]broker.StreamSubscriptionHandler),
	}
}

// Notify finds all the handlers for the type of args and calls them.
func (t *mockBus) Notify(_ context.Context, args broker.Routeable) error {
	// Get handlers safely under read lock
	t.mu.RLock()
	route := args.GetRoute()
	subs, exists := t.handlers[route]

	// Create a slice of handlers to execute outside the lock
	var handlersToExecute []broker.SubscriptionHandler
	if exists {
		handlersToExecute = make([]broker.SubscriptionHandler, 0, len(subs))
		for _, handler := range subs {
			handlersToExecute = append(handlersToExecute, handler)
		}
	}
	t.mu.RUnlock()

	// Execute handlers outside of any lock
	for _, handler := range handlersToExecute {
		go func(h broker.SubscriptionHandler) {
			// Execute in goroutine with a local copy of the handler
			h(args)
		}(handler)
	}

	return nil
}

// Subscribe registers a new handler for a specific type of message.
func (t *mockBus) Subscribe(ctx context.Context, route string, handler broker.SubscriptionHandler) (broker.Subscription, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.handlers[route]; !exists {
		t.handlers[route] = make(map[*TestSubscription]broker.SubscriptionHandler)
	}

	subscription := &TestSubscription{bus: t, key: route}
	t.handlers[route][subscription] = handler
	return subscription, nil
}

func (t *mockBus) CallStream(_ context.Context, args broker.Routeable) (broker.BidiStream, error) {
	// Get handlers safely under read lock
	t.mu.RLock()
	route := args.GetRoute()
	subs, exists := t.streamHandlers[route]

	var handlersToExecute []broker.StreamSubscriptionHandler
	if exists {
		handlersToExecute = make([]broker.StreamSubscriptionHandler, 0, len(subs))
		for _, handler := range subs {
			handlersToExecute = append(handlersToExecute, handler)
		}
	}
	t.mu.RUnlock()

	client := NewMockBidiStream()

	if len(handlersToExecute) > 0 {
		server := NewMockBidiStream()
		LinkStreams(client, server)

		r := CryptoRandInt(0, len(handlersToExecute))
		randomHandler := handlersToExecute[r]
		go randomHandler(args, server)
	} else {
		client.Close(nil)
	}

	return client, nil
}

// SubscribeToRequest implements server.Bus.
func (t *mockBus) SubscribeToStream(ctx context.Context, route string, handler broker.StreamSubscriptionHandler) (broker.Subscription, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.streamHandlers[route]; !exists {
		t.streamHandlers[route] = make(map[*TestSubscription]broker.StreamSubscriptionHandler)
	}

	subscription := &TestSubscription{bus: t, key: route}
	t.streamHandlers[route][subscription] = handler
	return subscription, nil
}

var _ broker.Subscription = &TestSubscription{}

type TestSubscription struct {
	bus *mockBus
	key string
}

// Unsubscribe removes the handler from mockBus.
func (t *TestSubscription) Unsubscribe() {
	t.bus.mu.Lock()
	defer t.bus.mu.Unlock()

	if subs, exists := t.bus.handlers[t.key]; exists {
		delete(subs, t)
		if len(subs) == 0 {
			delete(t.bus.handlers, t.key)
		}
	}

	if subs, exists := t.bus.streamHandlers[t.key]; exists {
		delete(subs, t)
		if len(subs) == 0 {
			delete(t.bus.streamHandlers, t.key)
		}
	}
}

func CryptoRandInt(min, max int) int {
	if min >= max {
		return 0
	}
	nBig, err := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	if err != nil {
		return 0
	}
	return int(nBig.Int64()) + min
}
