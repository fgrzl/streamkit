package bus

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fgrzl/json/polymorphic"
)

// MessageHandler processes an asynchronous message.
type MessageHandler func(ctx context.Context, msg Message) error

// Message represents an event-style message that does not expect a response.
type Message interface {
	polymorphic.Polymorphic
	GetRoute() Route
}

// Subscription represents an active subscription that can be unsubscribed.
type Subscription interface {
	Unsubscribe() error
}

// MessageBus defines the subset of message bus behavior streamkit needs.
type MessageBus interface {
	Notify(msg Message) error
	NotifyWithContext(ctx context.Context, msg Message) error
	Subscribe(route Route, handler MessageHandler) (Subscription, error)
	Close() error
}

// MessageBusFactory provides a way to retrieve a scoped MessageBus instance.
type MessageBusFactory interface {
	Get(ctx context.Context) (MessageBus, error)
}

// Subscribe registers a strongly-typed message handler on the given route.
func Subscribe[T Message](bus MessageBus, route Route, handler func(ctx context.Context, msg T) error) (Subscription, error) {
	return bus.Subscribe(route, func(ctx context.Context, msg Message) error {
		tMsg, ok := msg.(T)
		if !ok {
			slog.WarnContext(ctx,
				"Subscribe: unexpected message type",
				"expected", fmt.Sprintf("%T", *new(T)),
				"actual", fmt.Sprintf("%T", msg),
			)
			return fmt.Errorf("unexpected message type: %T", msg)
		}
		return handler(ctx, tMsg)
	})
}
