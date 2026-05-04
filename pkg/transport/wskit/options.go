// Package wskit provides WebSocket-based transport implementation
// for the streamkit streaming platform.
package wskit

import "time"

// MuxerOption configures a WebSocketMuxer before it starts processing traffic.
type MuxerOption func(*WebSocketMuxer)

// StreamRecvSaturationPolicy controls what happens when a logical stream
// receive queue remains saturated beyond the configured timeout threshold.
type StreamRecvSaturationPolicy int

const (
	// StreamRecvSaturationPolicyWait keeps waiting for receive-buffer headroom
	// and avoids force-closing slow consumers.
	StreamRecvSaturationPolicyWait StreamRecvSaturationPolicy = iota + 1
	// StreamRecvSaturationPolicyClose force-closes saturated streams after the
	// configured timeout threshold has been exceeded.
	StreamRecvSaturationPolicyClose
)

// WithWriteQueueSize sets the outbound multiplexed write channel buffer depth.
// Values less than or equal to zero are ignored.
func WithWriteQueueSize(size int) MuxerOption {
	return func(m *WebSocketMuxer) {
		if size > 0 {
			m.writeQueueSize = size
		}
	}
}

// WithStreamRecvQueueSize sets the inbound buffer size for logical streams.
// Values less than or equal to zero are ignored.
func WithStreamRecvQueueSize(size int) MuxerOption {
	return func(m *WebSocketMuxer) {
		if size > 0 {
			m.streamRecvQueueSize = size
		}
	}
}

// WithStreamIngressQueueSize sets the per-logical-stream ingress backlog used
// between the shared websocket read loop and the stream's receive queue.
// Values less than or equal to zero are ignored.
func WithStreamIngressQueueSize(size int) MuxerOption {
	return func(m *WebSocketMuxer) {
		if size > 0 {
			m.streamIngressQueueSize = size
		}
	}
}

// WithStreamRecvOfferTimeout sets how long the muxer waits for a logical stream
// to free receive-buffer headroom before retrying or closing the channel.
// Values less than or equal to zero are ignored.
func WithStreamRecvOfferTimeout(timeout time.Duration) MuxerOption {
	return func(m *WebSocketMuxer) {
		if timeout > 0 {
			m.streamRecvOfferTimeout = timeout
		}
	}
}

// WithStreamRecvSaturationThreshold sets how many consecutive receive-buffer
// timeouts are tolerated before the muxer tombstones the stream.
// Values less than or equal to zero are ignored.
func WithStreamRecvSaturationThreshold(threshold int64) MuxerOption {
	return func(m *WebSocketMuxer) {
		if threshold > 0 {
			m.streamRecvSaturationThreshold = threshold
		}
	}
}

// WithStreamRecvSaturationPolicy sets the sustained receive-buffer saturation
// policy for logical streams.
func WithStreamRecvSaturationPolicy(policy StreamRecvSaturationPolicy) MuxerOption {
	return func(m *WebSocketMuxer) {
		if policy == StreamRecvSaturationPolicyWait || policy == StreamRecvSaturationPolicyClose {
			m.streamRecvSaturationPolicy = policy
		}
	}
}

// WithMuxerTombstoneMax sets the maximum number of tombstone entries per muxer.
// Values less than or equal to zero disable the size cap (age-based pruning still applies if configured).
func WithMuxerTombstoneMax(n int) MuxerOption {
	return func(m *WebSocketMuxer) {
		m.tombstoneMax = n
	}
}

// WithMuxerTombstoneMaxAge sets how long tombstone entries are retained to ignore late frames.
// Values less than or equal to zero disable age-based eviction (size cap still applies if configured).
func WithMuxerTombstoneMaxAge(d time.Duration) MuxerOption {
	return func(m *WebSocketMuxer) {
		m.tombstoneMaxAge = d
	}
}

// WithMuxerNowFunc overrides the clock used for tombstone timestamps and pruning (tests only).
func WithMuxerNowFunc(fn func() time.Time) MuxerOption {
	return func(m *WebSocketMuxer) {
		m.nowFn = fn
	}
}

func applyMuxerOptions(m *WebSocketMuxer, opts ...MuxerOption) {
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
}

func streamRecvSaturationPolicyString(policy StreamRecvSaturationPolicy) string {
	switch policy {
	case StreamRecvSaturationPolicyWait:
		return "wait"
	case StreamRecvSaturationPolicyClose:
		return "close"
	default:
		return "unknown"
	}
}
