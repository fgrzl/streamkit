package streamkit

import (
	"context"
	"log/slog"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
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

type ClientFactory interface {
	Get(ctx context.Context) (Client, error)
}

type Client interface {
	GetSpaces(ctx context.Context, storeID uuid.UUID) enumerators.Enumerator[string]
	GetSegments(ctx context.Context, storeID uuid.UUID, space string) enumerators.Enumerator[string]
	Peek(ctx context.Context, storeID uuid.UUID, space, segment string) (*Entry, error)
	Consume(ctx context.Context, storeID uuid.UUID, args *Consume) enumerators.Enumerator[*Entry]
	ConsumeSpace(ctx context.Context, storeID uuid.UUID, args *ConsumeSpace) enumerators.Enumerator[*Entry]
	ConsumeSegment(ctx context.Context, storeID uuid.UUID, args *ConsumeSegment) enumerators.Enumerator[*Entry]
	Produce(ctx context.Context, storeID uuid.UUID, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus]
	Publish(ctx context.Context, storeID uuid.UUID, space, segment string, payload []byte, metadata map[string]string) error
	SubscribeToSpace(ctx context.Context, storeID uuid.UUID, space string, handler func(*SegmentStatus)) (api.Subscription, error)
	SubscribeToSegment(ctx context.Context, storeID uuid.UUID, space, segment string, handler func(*SegmentStatus)) (api.Subscription, error)
}

func NewClient(provider api.BidiStreamProvider) Client {
	return &client{provider: provider}
}

type client struct {
	provider api.BidiStreamProvider
}

func (c *client) GetSpaces(ctx context.Context, storeID uuid.UUID) enumerators.Enumerator[string] {
	bidi, err := c.provider.CallStream(ctx, storeID, &api.GetSpaces{})
	if err != nil {
		return enumerators.Error[string](err)
	}
	return api.NewStreamEnumerator[string](bidi)
}

func (c *client) GetSegments(ctx context.Context, storeID uuid.UUID, space string) enumerators.Enumerator[string] {
	bidi, err := c.provider.CallStream(ctx, storeID, &api.GetSegments{Space: space})
	if err != nil {
		return enumerators.Error[string](err)
	}
	return api.NewStreamEnumerator[string](bidi)
}

func (c *client) ConsumeSpace(ctx context.Context, storeID uuid.UUID, args *api.ConsumeSpace) enumerators.Enumerator[*Entry] {
	bidi, err := c.provider.CallStream(ctx, storeID, args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return api.NewStreamEnumerator[*Entry](bidi)
}

func (c *client) ConsumeSegment(ctx context.Context, storeID uuid.UUID, args *api.ConsumeSegment) enumerators.Enumerator[*Entry] {
	bidi, err := c.provider.CallStream(ctx, storeID, args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return api.NewStreamEnumerator[*Entry](bidi)
}

func (c *client) Consume(ctx context.Context, storeID uuid.UUID, args *api.Consume) enumerators.Enumerator[*Entry] {
	bidi, err := c.provider.CallStream(ctx, storeID, args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return api.NewStreamEnumerator[*Entry](bidi)
}

func (c *client) Peek(ctx context.Context, storeID uuid.UUID, space, segment string) (*Entry, error) {
	bidi, err := c.provider.CallStream(ctx, storeID, &api.Peek{Space: space, Segment: segment})
	if err != nil {
		return nil, err
	}
	//defer stream.Close(nil)
	entry := &api.Entry{}
	if err := bidi.Decode(&entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (c *client) Produce(ctx context.Context, storeID uuid.UUID, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus] {
	bidi, err := c.provider.CallStream(ctx, storeID, &api.Produce{Space: space, Segment: segment})
	if err != nil {
		return enumerators.Error[*SegmentStatus](err)
	}

	go func(bidi api.BidiStream, entries enumerators.Enumerator[*Record]) {
		defer entries.Dispose()
		for entries.MoveNext() {
			entry, err := entries.Current()
			if err != nil {
				bidi.CloseSend(err)
				return
			}
			if err := bidi.Encode(entry); err != nil {
				bidi.CloseSend(err)
				return
			}
		}
		bidi.CloseSend(entries.Err())
	}(bidi, entries)

	return api.NewStreamEnumerator[*SegmentStatus](bidi)
}

func (c *client) Publish(ctx context.Context, storeID uuid.UUID, space, segment string, payload []byte, metadata map[string]string) error {
	peek, err := c.Peek(ctx, storeID, space, segment)
	if err != nil {
		return err
	}

	bidi, err := c.provider.CallStream(ctx, storeID, &api.Produce{Space: space, Segment: segment})
	if err != nil {
		return err
	}
	defer bidi.Close(nil)

	record := &api.Record{
		Sequence: peek.Sequence + 1,
		Payload:  payload,
		Metadata: metadata,
	}

	if err := bidi.Encode(record); err != nil {
		bidi.CloseSend(err)
		return err
	}
	bidi.CloseSend(nil)

	enumerator := api.NewStreamEnumerator[*SegmentStatus](bidi)
	if err := enumerators.Consume(enumerator); err != nil {
		bidi.Close(err)
		return err
	}

	return nil
}

func (c *client) SubscribeToSpace(ctx context.Context, storeID uuid.UUID, space string, handler func(*SegmentStatus)) (api.Subscription, error) {
	args := &api.SubscribeToSegmentStatus{Space: space, Segment: "*"}
	return c.subscribeStream(ctx, storeID, args, handler)
}

func (c *client) SubscribeToSegment(ctx context.Context, storeID uuid.UUID, space, segment string, handler func(*SegmentStatus)) (api.Subscription, error) {
	args := &api.SubscribeToSegmentStatus{Space: space, Segment: segment}
	return c.subscribeStream(ctx, storeID, args, handler)
}

func (c *client) subscribeStream(ctx context.Context, storeID uuid.UUID, initMsg api.Routeable, handler func(*SegmentStatus)) (api.Subscription, error) {
	bidi, err := c.provider.CallStream(ctx, storeID, initMsg)
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
				bidi.Close(ctx.Err())
				return
			default:
				var status SegmentStatus
				if err := bidi.Decode(&status); err != nil {
					slog.ErrorContext(ctx, "subscription closed", "err", err)
					bidi.Close(err)
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
