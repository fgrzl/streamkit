// Package client provides a high-throughput, hierarchical event streaming client
// for interacting with streamkit servers.
//
// The package offers a client interface for interacting with streaming stores,
// spaces, and segments. A Store provides physical separation at the storage level,
// acting as the root for all spaces and segments. A Space is a top-level logical
// container for related streams, while Segments are independent, ordered sub-streams
// within a Space.
package client

import (
	"context"
	"log/slog"
	"time"

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

	// Interfaces
	Consumable = api.Consumable
	Routeable  = api.Routeable
)

// ClientFactory defines how to create new Client instances.
type ClientFactory interface {
	Get(ctx context.Context) (Client, error)
}

// Client provides the main interface for interacting with the streaming platform.
// It supports operations for spaces, segments, consumption, production, and subscriptions.
type Client interface {
	// GetSpaces returns an enumerator of space names for the given store.
	GetSpaces(ctx context.Context, storeID uuid.UUID) enumerators.Enumerator[string]

	// GetSegments returns an enumerator of segment names within the specified space.
	GetSegments(ctx context.Context, storeID uuid.UUID, space string) enumerators.Enumerator[string]

	// Peek returns the latest entry in the specified segment without consuming it.
	Peek(ctx context.Context, storeID uuid.UUID, space, segment string) (*Entry, error)

	// Consume reads entries from multiple spaces based on the provided consumption arguments.
	Consume(ctx context.Context, storeID uuid.UUID, args *Consume) enumerators.Enumerator[*Entry]

	// ConsumeSpace reads entries from all segments within a space, returning them in timestamp order.
	ConsumeSpace(ctx context.Context, storeID uuid.UUID, args *ConsumeSpace) enumerators.Enumerator[*Entry]

	// ConsumeSegment reads entries from a specific segment in sequence order.
	ConsumeSegment(ctx context.Context, storeID uuid.UUID, args *ConsumeSegment) enumerators.Enumerator[*Entry]

	// Produce writes a stream of records to the specified segment and returns status updates.
	Produce(ctx context.Context, storeID uuid.UUID, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus]

	// Publish writes a single record to the specified segment.
	Publish(ctx context.Context, storeID uuid.UUID, space, segment string, payload []byte, metadata map[string]string) error

	// SubscribeToSpace subscribes to status updates for all segments within a space.
	SubscribeToSpace(ctx context.Context, storeID uuid.UUID, space string, handler func(*SegmentStatus)) (api.Subscription, error)

	// SubscribeToSegment subscribes to status updates for a specific segment.
	SubscribeToSegment(ctx context.Context, storeID uuid.UUID, space, segment string, handler func(*SegmentStatus)) (api.Subscription, error)
}

// NewClient creates a new Client instance using the provided BidiStreamProvider.
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
	defer bidi.Close(nil)

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
		count := 0
		for entries.MoveNext() {
			entry, err := entries.Current()
			if err != nil {
				slog.Error("produce: failed to get current entry", "err", err, "count", count)
				bidi.CloseSend(err)
				bidi.Close(err) // Close local stream so status enumerator fails immediately
				return
			}
			if err := bidi.Encode(entry); err != nil {
				slog.Error("produce: failed to encode entry", "err", err, "count", count)
				bidi.CloseSend(err)
				bidi.Close(err) // Close local stream so status enumerator fails immediately
				return
			}
			count++
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
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		backoff := time.Second
		maxBackoff := 30 * time.Second
		attempt := 0

		for {
			// Check for cancellation before attempting (re)subscription
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Create new stream (initial or reconnect)
			attempt++
			stream, err := c.provider.CallStream(ctx, storeID, initMsg)
			if err != nil {
				slog.WarnContext(ctx, "subscription stream failed, retrying", "attempt", attempt, "backoff", backoff, "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
					// Exponential backoff with max
					backoff = backoff * 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					continue
				}
			}

			// Reset backoff on successful connection
			backoff = time.Second
			slog.InfoContext(ctx, "subscription stream connected", "attempt", attempt)

			// Read from stream until it fails
			for {
				select {
				case <-ctx.Done():
					stream.Close(ctx.Err())
					return
				default:
				}

				var status SegmentStatus
				if err := stream.Decode(&status); err != nil {
					slog.WarnContext(ctx, "subscription decode failed, reconnecting", "err", err)
					stream.Close(err)
					break // inner loop - will reconnect
				}
				handler(&status)
			}
			// Stream failed, loop will retry
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
