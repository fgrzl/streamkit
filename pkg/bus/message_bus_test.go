package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSubscription struct{}

func (s *testSubscription) Unsubscribe() error {
	return nil
}

type testMessageBus struct {
	route          Route
	handler        MessageHandler
	subscribeErr   error
	subscription   Subscription
	subscribeCalls int
}

func (b *testMessageBus) Notify(msg Message) error {
	return nil
}

func (b *testMessageBus) NotifyWithContext(ctx context.Context, msg Message) error {
	return nil
}

func (b *testMessageBus) Subscribe(route Route, handler MessageHandler) (Subscription, error) {
	b.route = route
	b.handler = handler
	b.subscribeCalls++
	if b.subscription == nil && b.subscribeErr == nil {
		b.subscription = &testSubscription{}
	}
	return b.subscription, b.subscribeErr
}

func (b *testMessageBus) Close() error {
	return nil
}

type testMessage struct {
	route Route
	value string
}

func (m *testMessage) GetDiscriminator() string {
	return "streamkit://tests/bus/test_message"
}

func (m *testMessage) GetRoute() Route {
	return m.route
}

type otherTestMessage struct {
	route Route
}

func (m *otherTestMessage) GetDiscriminator() string {
	return "streamkit://tests/bus/other_message"
}

func (m *otherTestMessage) GetRoute() Route {
	return m.route
}

func TestShouldDelegateToUnderlyingBusWhenSubscribe(t *testing.T) {
	route := NewGlobalRoute("streams", "segment-updated")
	bus := &testMessageBus{}

	subscription, err := Subscribe(bus, route, func(ctx context.Context, msg *testMessage) error {
		return nil
	})

	require.NoError(t, err)
	require.NotNil(t, subscription)
	assert.Equal(t, route, bus.route)
	assert.NotNil(t, bus.handler)
	assert.Equal(t, 1, bus.subscribeCalls)
}

func TestShouldDispatchMatchingTypedMessagesWhenSubscribe(t *testing.T) {
	route := NewGlobalRoute("streams", "segment-updated")
	bus := &testMessageBus{}
	message := &testMessage{route: route, value: "ok"}

	var handled *testMessage
	_, err := Subscribe(bus, route, func(ctx context.Context, msg *testMessage) error {
		handled = msg
		return nil
	})
	require.NoError(t, err)

	err = bus.handler(context.Background(), message)

	require.NoError(t, err)
	assert.Same(t, message, handled)
}

func TestShouldReturnErrorForUnexpectedMessageTypeWhenSubscribe(t *testing.T) {
	route := NewGlobalRoute("streams", "segment-updated")
	bus := &testMessageBus{}

	_, err := Subscribe(bus, route, func(ctx context.Context, msg *testMessage) error {
		return nil
	})
	require.NoError(t, err)

	err = bus.handler(context.Background(), &otherTestMessage{route: route})

	require.Error(t, err)
	assert.ErrorContains(t, err, "unexpected message type")
	assert.ErrorContains(t, err, "otherTestMessage")
}

func TestShouldPropagateUnderlyingSubscribeErrorWhenSubscribe(t *testing.T) {
	expectedErr := errors.New("subscribe failed")
	bus := &testMessageBus{subscribeErr: expectedErr}
	route := NewGlobalRoute("streams", "segment-updated")

	subscription, err := Subscribe(bus, route, func(ctx context.Context, msg *testMessage) error {
		return nil
	})

	require.ErrorIs(t, err, expectedErr)
	assert.Nil(t, subscription)
	assert.Equal(t, 1, bus.subscribeCalls)
}
