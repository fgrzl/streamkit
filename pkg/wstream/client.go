package wstream

import (
	"context"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit"
	"github.com/fgrzl/streamkit/pkg/api"
)

func NewClient(bus api.Bus) streamkit.Client {
	return &WSStreamClient{
		bus: bus,
	}
}

type WSStreamClient struct {
	bus api.Bus
}

func (d *WSStreamClient) GetClusterStatus(ctx context.Context) (*api.ClusterStatus, error) {
	stream, err := d.bus.CallStream(ctx, &api.GetStatus{})
	if err != nil {
		return nil, err
	}
	entry := &api.ClusterStatus{}
	if err := stream.Decode(&entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (d *WSStreamClient) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	stream, err := d.bus.CallStream(ctx, &api.GetSpaces{})
	if err != nil {
		return enumerators.Error[string](err)
	}
	return api.NewStreamEnumerator[string](stream)
}

func (d *WSStreamClient) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	stream, err := d.bus.CallStream(ctx, args)
	if err != nil {
		return enumerators.Error[*api.Entry](err)
	}
	return api.NewStreamEnumerator[*api.Entry](stream)
}

func (d *WSStreamClient) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	stream, err := d.bus.CallStream(ctx, &api.GetSegments{Space: space})
	if err != nil {
		return enumerators.Error[string](err)
	}
	return api.NewStreamEnumerator[string](stream)
}

func (d *WSStreamClient) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	stream, err := d.bus.CallStream(ctx, args)
	if err != nil {
		return enumerators.Error[*api.Entry](err)
	}
	return api.NewStreamEnumerator[*api.Entry](stream)
}

func (d *WSStreamClient) Peek(ctx context.Context, space string, segment string) (*api.Entry, error) {
	stream, err := d.bus.CallStream(ctx, &api.Peek{Space: space, Segment: segment})
	if err != nil {
		return nil, err
	}
	entry := &api.Entry{}
	if err := stream.Decode(&entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (d *WSStreamClient) Produce(ctx context.Context, space, segment string, entries enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	stream, err := d.bus.CallStream(ctx, &api.Produce{Space: space, Segment: segment})
	if err != nil {
		return enumerators.Error[*api.SegmentStatus](err)
	}

	go func(s api.BidiStream, entries enumerators.Enumerator[*api.Record]) {
		defer entries.Dispose()
		for entries.MoveNext() {
			entry, err := entries.Current()
			if err != nil {
				s.CloseSend(err)
			}
			if err := s.Encode(entry); err != nil {
				s.CloseSend(err)
			}
		}
		s.CloseSend(nil)
	}(stream, entries)

	return api.NewStreamEnumerator[*api.SegmentStatus](stream)
}

func (d *WSStreamClient) Publish(ctx context.Context, space, segment string, payload []byte, metadata map[string]string) error {
	peek, err := d.Peek(ctx, space, segment)
	if err != nil {
		return err
	}

	stream, err := d.bus.CallStream(ctx, &api.Produce{Space: space, Segment: segment})
	if err != nil {
		return err
	}

	record := &api.Record{
		Sequence: peek.Sequence + 1,
		Payload:  payload,
		Metadata: metadata,
	}

	if err := stream.Encode(record); err != nil {
		stream.CloseSend(err)
	}
	stream.CloseSend(nil)
	enumerator := api.NewStreamEnumerator[*api.SegmentStatus](stream)

	if err := enumerators.Consume(enumerator); err != nil {
		stream.Close(err)
		return err
	}
	stream.Close(nil)
	return nil
}

func (d *WSStreamClient) Consume(ctx context.Context, args *api.Consume) enumerators.Enumerator[*api.Entry] {
	stream, err := d.bus.CallStream(ctx, args)
	if err != nil {
		return enumerators.Error[*api.Entry](err)
	}
	return api.NewStreamEnumerator[*api.Entry](stream)
}

func (d *WSStreamClient) SubcribeToSpace(
	ctx context.Context,
	space string,
	handler func(*api.SegmentStatus),
) (api.Subscription, error) {

	args := &api.SubscribeToSpaceNotification{
		Space: space,
	}

	stream, err := d.bus.CallStream(ctx, args)
	if err != nil {
		return nil, err
	}

	// Cancellation support
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				stream.Close(ctx.Err())
				return
			default:
				var status api.SegmentStatus
				if err := stream.Decode(&status); err != nil {
					// TODO: log or reconnect based on strategy
					stream.Close(err)
					return
				}
				handler(&status)
			}
		}
	}()

	return &subscription{
		cancel: cancel,
		done:   done,
	}, nil
}

func (d *WSStreamClient) SubcribeToSegment(ctx context.Context, space, segment string, handler func(*api.SegmentStatus)) (api.Subscription, error) {
	panic("implement me")
}

type subscription struct {
	cancel func()
	done   <-chan struct{}
}

func (s *subscription) Unsubscribe() {
	s.cancel()
	<-s.done // Wait for background goroutine to exit
}
