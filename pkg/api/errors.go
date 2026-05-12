package api

var (
	// ErrCommitBatch is returned when a batch commit fails in a retryable way.
	ErrCommitBatch = NewTransientError("failed to commit batch")
	// ErrGetTransaction is returned when loading a transaction fails transiently.
	ErrGetTransaction = NewTransientError("failed to get transaction")
	// ErrMarshalEntry is returned when serializing an entry fails transiently.
	ErrMarshalEntry = NewTransientError("failed to marshal entry")
	// ErrMarshalTrx is returned when serializing a transaction fails transiently.
	ErrMarshalTrx = NewTransientError("failed to marshal transaction")
	// ErrOpenDB is returned when opening local storage fails transiently.
	ErrOpenDB = NewTransientError("failed to open pebble db")
	// ErrSeqNumberAhead is returned when the node sequence is ahead of the client.
	ErrSeqNumberAhead = NewTransientError("sequence number mismatch, the node is ahead")
	// ErrSeqNumberBehind is returned when the node sequence is behind the client.
	ErrSeqNumberBehind = NewTransientError("sequence number mismatch, the node is behind")
	// ErrSequenceMismatch is returned when a permanent sequence mismatch is detected.
	ErrSequenceMismatch = NewPermanentError("sequence mismatch")
	// ErrSetEntrySegment is returned when writing an entry to a segment fails transiently.
	ErrSetEntrySegment = NewTransientError("failed to set entry in segment")
	// ErrSetEntrySpace is returned when writing an entry to a space fails transiently.
	ErrSetEntrySpace = NewTransientError("failed to set entry in space")
	// ErrTrxEmpty is returned when a transaction has no entries.
	ErrTrxEmpty = NewTransientError("transaction is empty")
	// ErrTrxIDMismatch is returned when a transaction ID does not match expectations.
	ErrTrxIDMismatch = NewPermanentError("Transaction ID mismatch")
	// ErrTrxNotFound is returned when a transaction record is missing transiently.
	ErrTrxNotFound = NewTransientError("transaction not found")
	// ErrTrxNumberAhead is returned when the transaction number is ahead on the node.
	ErrTrxNumberAhead = NewPermanentError("transaction number mismatch, the node is ahead")
	// ErrTrxNumberBehind is returned when the transaction number is behind on the node.
	ErrTrxNumberBehind = NewPermanentError("transaction number mismatch, the node is behind")
	// ErrTrxPending is returned when a transaction is still pending.
	ErrTrxPending = NewTransientError("transaction is pending")
	// ErrUnmarshalTrx is returned when decoding a transaction fails transiently.
	ErrUnmarshalTrx = NewTransientError("failed to unmarshal transaction")
	// ErrUpdateSegmentInventory is returned when segment inventory update fails transiently.
	ErrUpdateSegmentInventory = NewTransientError("failed to update segment inventory")
	// ErrUpdateSpaceInventory is returned when space inventory update fails transiently.
	ErrUpdateSpaceInventory = NewTransientError("failed to update space inventory")
	// ErrWriteTrx is returned when persisting a transaction fails transiently.
	ErrWriteTrx = NewTransientError("failed to write transaction")
)

// StreamsError represents errors that occur within the streaming system.
type StreamsError struct {
	Code    int
	Message string
}

// Error returns the error message string.
func (e *StreamsError) Error() string {
	return e.Message
}

const (
	// ErrCodeTransient indicates a temporary error that may be retried.
	ErrCodeTransient = 1
	// ErrCodePermanent indicates a permanent error that should not be retried.
	ErrCodePermanent = 2
)

// NewTransientError creates a new transient error with the given message.
func NewTransientError(msg string) error {
	return &StreamsError{Code: ErrCodeTransient, Message: msg}
}

// NewPermanentError creates a new permanent error with the given message.
func NewPermanentError(msg string) error {
	return &StreamsError{Code: ErrCodePermanent, Message: msg}
}
