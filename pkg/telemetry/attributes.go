package telemetry

import (
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
)

// Standard attribute keys for streamkit spans.
const (
	AttrStoreID      = attribute.Key("streamkit.store_id")
	AttrSpace        = attribute.Key("streamkit.space")
	AttrSegment      = attribute.Key("streamkit.segment")
	AttrRequestID    = attribute.Key("streamkit.request_id")
	AttrConnectionID = attribute.Key("streamkit.connection_id")
	AttrChannelID    = attribute.Key("streamkit.channel_id")

	// Operation-specific attributes
	AttrRecordCount     = attribute.Key("streamkit.record_count")
	AttrResultCount     = attribute.Key("streamkit.result_count")
	AttrPayloadSize     = attribute.Key("streamkit.payload_size")
	AttrSequenceStart   = attribute.Key("streamkit.sequence_start")
	AttrSequenceEnd     = attribute.Key("streamkit.sequence_end")
	AttrTimestampMin    = attribute.Key("streamkit.timestamp_min")
	AttrTimestampMax    = attribute.Key("streamkit.timestamp_max")
	AttrTransactionID   = attribute.Key("streamkit.transaction_id")
	AttrTransactionNum  = attribute.Key("streamkit.transaction_num")
	AttrBatchSize       = attribute.Key("streamkit.batch_size")
	AttrChunkCount      = attribute.Key("streamkit.chunk_count")
	AttrAttempt         = attribute.Key("streamkit.attempt")
	AttrBackoffDuration = attribute.Key("streamkit.backoff_duration_ms")
	AttrErrorCode       = attribute.Key("streamkit.error_code")
	AttrHandlerDuration = attribute.Key("streamkit.handler_duration_ms")
	AttrCacheHit        = attribute.Key("streamkit.cache_hit")

	// Subscription attributes
	AttrSubscriptionID = attribute.Key("streamkit.subscription_id")
	AttrHandlerStatus  = attribute.Key("streamkit.handler_status") // "success", "panic", "timeout"

	// Transport attributes
	AttrTransportType = attribute.Key("streamkit.transport_type") // "inproc", "websocket"
	AttrMessageType   = attribute.Key("streamkit.message_type")   // "consume", "produce", etc.

	// Storage backend attributes
	AttrBackendType  = attribute.Key("streamkit.backend_type") // "pebbledb", "azure"
	AttrPartitionKey = attribute.Key("streamkit.partition_key")
	AttrRangeQuery   = attribute.Key("streamkit.range_query")
)

// Attribute builder functions for convenience and consistency.

// WithStoreID returns an attribute for the store ID.
func WithStoreID(id uuid.UUID) attribute.KeyValue {
	return AttrStoreID.String(id.String())
}

// WithSpace returns an attribute for the space name.
func WithSpace(space string) attribute.KeyValue {
	return AttrSpace.String(space)
}

// WithSegment returns an attribute for the segment name.
func WithSegment(segment string) attribute.KeyValue {
	return AttrSegment.String(segment)
}

// WithConnectionID returns an attribute for the connection ID.
func WithConnectionID(id uuid.UUID) attribute.KeyValue {
	return AttrConnectionID.String(id.String())
}

// WithRequestID returns an attribute for the request ID.
func WithRequestID(id uuid.UUID) attribute.KeyValue {
	return AttrRequestID.String(id.String())
}

// WithChannelID returns an attribute for the channel ID.
func WithChannelID(id uuid.UUID) attribute.KeyValue {
	return AttrChannelID.String(id.String())
}

// WithRecordCount returns an attribute for the number of records.
func WithRecordCount(count int) attribute.KeyValue {
	return AttrRecordCount.Int(count)
}

// WithResultCount returns an attribute for the number of results.
func WithResultCount(count int) attribute.KeyValue {
	return AttrResultCount.Int(count)
}

// WithPayloadSize returns an attribute for payload size in bytes.
func WithPayloadSize(size int) attribute.KeyValue {
	return AttrPayloadSize.Int(size)
}

// WithSequenceRange returns attributes for sequence start and end.
func WithSequenceRange(start, end uint64) []attribute.KeyValue {
	return []attribute.KeyValue{
		AttrSequenceStart.Int64(int64(start)),
		AttrSequenceEnd.Int64(int64(end)),
	}
}

// WithTimestampRange returns attributes for timestamp min and max.
func WithTimestampRange(min, max int64) []attribute.KeyValue {
	return []attribute.KeyValue{
		AttrTimestampMin.Int64(min),
		AttrTimestampMax.Int64(max),
	}
}

// WithTransactionID returns an attribute for the transaction ID.
func WithTransactionID(id uuid.UUID) attribute.KeyValue {
	return AttrTransactionID.String(id.String())
}

// WithTransactionNum returns an attribute for the transaction number.
func WithTransactionNum(num uint64) attribute.KeyValue {
	return AttrTransactionNum.Int64(int64(num))
}

// WithBatchSize returns an attribute for batch size.
func WithBatchSize(size int) attribute.KeyValue {
	return AttrBatchSize.Int(size)
}

// WithChunkCount returns an attribute for chunk count.
func WithChunkCount(count int) attribute.KeyValue {
	return AttrChunkCount.Int(count)
}

// WithAttempt returns an attribute for attempt number (1-indexed).
func WithAttempt(num int) attribute.KeyValue {
	return AttrAttempt.Int(num)
}

// WithBackoffDuration returns an attribute for backoff duration in milliseconds.
func WithBackoffDuration(ms int64) attribute.KeyValue {
	return AttrBackoffDuration.Int64(ms)
}

// WithCacheHit returns an attribute indicating cache hit status.
func WithCacheHit(hit bool) attribute.KeyValue {
	return AttrCacheHit.Bool(hit)
}

// WithTransportType returns an attribute for transport type.
func WithTransportType(transportType string) attribute.KeyValue {
	return AttrTransportType.String(transportType)
}

// WithMessageType returns an attribute for message type.
func WithMessageType(msgType string) attribute.KeyValue {
	return AttrMessageType.String(msgType)
}

// WithBackendType returns an attribute for storage backend type.
func WithBackendType(backendType string) attribute.KeyValue {
	return AttrBackendType.String(backendType)
}
