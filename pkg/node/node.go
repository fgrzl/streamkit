package node

import (
	"context"
	"fmt"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/storage"
)

type Node interface {
	Handle(context.Context, api.BidiStream)
	Close()
}

func NewNode(store storage.Store) Node {
	return &defaultNode{
		store: store,
	}
}

type defaultNode struct {
	store storage.Store
}

func (n *defaultNode) Close() {
	n.store.Close()
}

func (n *defaultNode) Handle(ctx context.Context, bidi api.BidiStream) {

	defer func() {
		if r := recover(); r != nil {
			bidi.Close(fmt.Errorf("panic: %v", r))
		}
	}()

	envelope := &polymorphic.Envelope{}
	if err := bidi.Decode(envelope); err != nil {
		bidi.Close(err)
		return
	}

	switch args := envelope.Content.(type) {
	case *api.Consume:
		n.handleConsume(ctx, args, bidi)
	case *api.ConsumeSegment:
		n.handleConsumeSegment(ctx, args, bidi)
	case *api.ConsumeSpace:
		n.handleConsumeSpace(ctx, args, bidi)
	case *api.GetSegments:
		n.handleGetSegments(ctx, args, bidi)
	case *api.GetSpaces:
		n.handleGetSpaces(ctx, args, bidi)
	case *api.Peek:
		n.handlePeek(ctx, args, bidi)
	case *api.Produce:
		n.handleProduce(ctx, args, bidi)
	default:
		bidi.Close(fmt.Errorf("invalid request msg type: %T", envelope.Content))
	}
}

func (n *defaultNode) handleGetSpaces(ctx context.Context, _ *api.GetSpaces, bidi api.BidiStream) {
	enumerator := n.store.GetSpaces(ctx)
	streamNames(ctx, enumerator, bidi)
}

func (n *defaultNode) handleConsumeSpace(ctx context.Context, args *api.ConsumeSpace, bidi api.BidiStream) {
	enumerator := n.store.ConsumeSpace(ctx, args)
	streamEntries(ctx, enumerator, bidi)
}

func (n *defaultNode) handleGetSegments(ctx context.Context, args *api.GetSegments, bidi api.BidiStream) {
	enumerator := n.store.GetSegments(ctx, args.Space)
	streamNames(ctx, enumerator, bidi)
}

func (n *defaultNode) handleConsumeSegment(ctx context.Context, args *api.ConsumeSegment, bidi api.BidiStream) {
	enumerator := n.store.ConsumeSegment(ctx, args)
	streamEntries(ctx, enumerator, bidi)
}

func (n *defaultNode) handlePeek(ctx context.Context, args *api.Peek, bidi api.BidiStream) {
	if !checkContext(ctx, bidi) {
		return
	}
	entry, err := n.store.Peek(ctx, args.Space, args.Segment)
	if err != nil {
		bidi.Close(err)
		return
	}
	if err := bidi.Encode(entry); err != nil {
		bidi.Close(err)
		return
	}
	bidi.Close(nil)
}

func (n *defaultNode) handleProduce(ctx context.Context, args *api.Produce, bidi api.BidiStream) {
	entries := api.NewStreamEnumerator[*api.Record](bidi)
	results := n.store.Produce(ctx, args, entries)
	defer results.Dispose()

	for results.MoveNext() {
		if !checkContext(ctx, bidi) {
			return
		}
		result, err := results.Current()
		if err != nil {
			bidi.CloseSend(err)
			return
		}
		if err := bidi.Encode(result); err != nil {
			bidi.CloseSend(err)
			return
		}
	}
	bidi.CloseSend(nil)
}

func (n *defaultNode) handleConsume(ctx context.Context, args *api.Consume, bidi api.BidiStream) {
	spaces := make([]enumerators.Enumerator[*api.Entry], 0, len(args.Offsets))
	for space, offset := range args.Offsets {
		spaces = append(spaces, n.store.ConsumeSpace(ctx, &api.ConsumeSpace{
			Space:        space,
			MinTimestamp: args.MinTimestamp,
			MaxTimestamp: args.MaxTimestamp,
			Offset:       offset,
		}))
	}
	enumerator := enumerators.Interleave(spaces, func(e *api.Entry) int64 { return e.Timestamp })
	streamEntries(ctx, enumerator, bidi)
}

func streamNames(ctx context.Context, enumerator enumerators.Enumerator[string], bidi api.BidiStream) {
	defer enumerator.Dispose()
	for enumerator.MoveNext() {
		if !checkContext(ctx, bidi) {
			return
		}
		name, err := enumerator.Current()
		if err != nil {
			bidi.CloseSend(err)
			return
		}
		if err := bidi.Encode(name); err != nil {
			bidi.CloseSend(err)
			return
		}
	}
	bidi.CloseSend(nil)
}

func streamEntries(ctx context.Context, enumerator enumerators.Enumerator[*api.Entry], bidi api.BidiStream) {
	defer enumerator.Dispose()
	for enumerator.MoveNext() {
		if !checkContext(ctx, bidi) {
			return
		}
		entry, err := enumerator.Current()
		if err != nil {
			bidi.CloseSend(err)
			return
		}
		if err := bidi.Encode(entry); err != nil {
			bidi.CloseSend(err)
			return
		}
	}
	bidi.CloseSend(nil)
}

func checkContext(ctx context.Context, bidi api.BidiStream) bool {
	if err := ctx.Err(); err != nil {
		bidi.CloseSend(err)
		return false
	}
	return true
}
