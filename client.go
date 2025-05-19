package streamkit

import (
	"context"
	"log/slog"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/api"
)

type (
	// Models
	Entry         = api.Entry
	Record        = api.Record
	SegmentStatus = api.SegmentStatus

	// Requests
	Consume        = api.Consume
	ConsumeSegment = api.ConsumeSegment
	ConsumeSpace   = api.ConsumeSpace
	GetSegments    = api.GetSegments
	GetSpaces      = api.GetSpaces
	GetStatus      = api.GetStatus
	Peek           = api.Peek
	Produce        = api.Produce
)

type Client interface {
	GetSpaces(ctx context.Context) enumerators.Enumerator[string]
	GetSegments(ctx context.Context, space string) enumerators.Enumerator[string]
	Peek(ctx context.Context, space, segment string) (*Entry, error)
	Consume(ctx context.Context, args *Consume) enumerators.Enumerator[*Entry]
	ConsumeSpace(ctx context.Context, args *ConsumeSpace) enumerators.Enumerator[*Entry]
	ConsumeSegment(ctx context.Context, args *ConsumeSegment) enumerators.Enumerator[*Entry]
	Produce(ctx context.Context, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus]
	Publish(ctx context.Context, space, segment string, payload []byte, metadata map[string]string) error
	SubscribeToSpace(ctx context.Context, space string, handler func(*SegmentStatus)) (api.Subscription, error)
	SubscribeToSegment(ctx context.Context, space, segment string, handler func(*SegmentStatus)) (api.Subscription, error)
}

func NewClient(provider api.BidiStreamProvider) Client {
	return &client{provider: provider}
}

type client struct {
	provider api.BidiStreamProvider
}

func (c *client) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	stream, err := c.provider.CallStream(ctx, &api.GetSpaces{})
	if err != nil {
		return enumerators.Error[string](err)
	}
	return api.NewStreamEnumerator[string](stream)
}

func (c *client) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	stream, err := c.provider.CallStream(ctx, &api.GetSegments{Space: space})
	if err != nil {
		return enumerators.Error[string](err)
	}
	return api.NewStreamEnumerator[string](stream)
}

func (c *client) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*Entry] {
	stream, err := c.provider.CallStream(ctx, args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return api.NewStreamEnumerator[*Entry](stream)
}

func (c *client) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*Entry] {
	stream, err := c.provider.CallStream(ctx, args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return api.NewStreamEnumerator[*Entry](stream)
}

func (c *client) Consume(ctx context.Context, args *api.Consume) enumerators.Enumerator[*Entry] {
	stream, err := c.provider.CallStream(ctx, args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return api.NewStreamEnumerator[*Entry](stream)
}

func (c *client) Peek(ctx context.Context, space, segment string) (*Entry, error) {
	stream, err := c.provider.CallStream(ctx, &api.Peek{Space: space, Segment: segment})
	if err != nil {
		return nil, err
	}
	defer stream.Close(nil)
	entry := &api.Entry{}
	if err := stream.Decode(&entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (c *client) Produce(ctx context.Context, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus] {
	stream, err := c.provider.CallStream(ctx, &api.Produce{Space: space, Segment: segment})
	if err != nil {
		return enumerators.Error[*SegmentStatus](err)
	}

	go func(s api.BidiStream, entries enumerators.Enumerator[*Record]) {
		defer entries.Dispose()
		for entries.MoveNext() {
			entry, err := entries.Current()
			if err != nil {
				s.CloseSend(err)
				return
			}
			if err := s.Encode(entry); err != nil {
				s.CloseSend(err)
				return
			}
		}
		s.CloseSend(nil)
	}(stream, entries)

	return api.NewStreamEnumerator[*SegmentStatus](stream)
}

func (c *client) Publish(ctx context.Context, space, segment string, payload []byte, metadata map[string]string) error {
	peek, err := c.Peek(ctx, space, segment)
	if err != nil {
		return err
	}

	stream, err := c.provider.CallStream(ctx, &api.Produce{Space: space, Segment: segment})
	if err != nil {
		return err
	}
	defer stream.Close(nil)

	record := &api.Record{
		Sequence: peek.Sequence + 1,
		Payload:  payload,
		Metadata: metadata,
	}

	if err := stream.Encode(record); err != nil {
		stream.CloseSend(err)
		return err
	}
	stream.CloseSend(nil)

	enumerator := api.NewStreamEnumerator[*SegmentStatus](stream)
	if err := enumerators.Consume(enumerator); err != nil {
		stream.Close(err)
		return err
	}

	return nil
}

func (c *client) SubscribeToSpace(ctx context.Context, space string, handler func(*SegmentStatus)) (api.Subscription, error) {
	args := &api.SubscribeToSpaceNotification{Space: space}
	return c.subscribeStream(ctx, args, handler)
}

func (c *client) SubscribeToSegment(ctx context.Context, space, segment string, handler func(*SegmentStatus)) (api.Subscription, error) {
	args := &api.SubscribeToSegmentNotification{Space: space, Segment: segment}
	return c.subscribeStream(ctx, args, handler)
}

func (c *client) subscribeStream(ctx context.Context, initMsg api.Routeable, handler func(*SegmentStatus)) (api.Subscription, error) {
	stream, err := c.provider.CallStream(ctx, initMsg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				slog.DebugContext(ctx, "subscription canceled")
				stream.Close(ctx.Err())
				return
			default:
				var status SegmentStatus
				if err := stream.Decode(&status); err != nil {
					slog.ErrorContext(ctx, "subscription closed", "err", err)
					stream.Close(err)
					return
				}
				handler(&status)
			}
		}
	}()

	return &subscription{cancel: cancel, done: done}, nil
}

type subscription struct {
	cancel func()
	done   <-chan struct{}
}

func (s *subscription) Unsubscribe() {
	s.cancel()
	<-s.done
}
