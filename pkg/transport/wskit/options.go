// Package wskit provides WebSocket-based transport implementation
// for the streamkit streaming platform.
package wskit

import "time"

// MuxerOption configures a WebSocketMuxer before it starts processing traffic.
type MuxerOption func(*WebSocketMuxer)

// WithStreamRecvQueueSize sets the inbound buffer size for logical streams.
// Values less than or equal to zero are ignored.
func WithStreamRecvQueueSize(size int) MuxerOption {
	return func(m *WebSocketMuxer) {
		if size > 0 {
			m.streamRecvQueueSize = size
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

func applyMuxerOptions(m *WebSocketMuxer, opts ...MuxerOption) {
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
}
