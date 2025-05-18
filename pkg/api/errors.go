package api

var (
	ERR_COMMIT_BATCH             error = NewTransientError("failed to commit batch")
	ERR_GET_TRANSACTION          error = NewTransientError("failed to get transaction")
	ERR_MARSHAL_ENTRY            error = NewTransientError("failed to marshal entry")
	ERR_MARSHAL_TRX              error = NewTransientError("failed to marshal transaction")
	ERR_OPEN_DB                  error = NewTransientError("failed to open pebble db")
	ERR_SEQ_NUMBER_AHEAD         error = NewTransientError("sequence number mismatch, the node is ahead")
	ERR_SEQ_NUMBER_BEHIND        error = NewTransientError("sequence number mismatch, the node is behind")
	ERR_SEQUENCE_MISMATCH        error = NewPermanentError("sequence mismatch")
	ERR_SET_ENTRY_SEGMENT        error = NewTransientError("failed to set entry in segment")
	ERR_SET_ENTRY_SPACE          error = NewTransientError("failed to set entry in space")
	ERR_TRX_EMPTY                error = NewTransientError("transaction is empty")
	ERR_TRX_ID_MISMATCH          error = NewPermanentError("Transaction ID mismatch")
	ERR_TRX_NOT_FOUND            error = NewTransientError("transaction not found")
	ERR_TRX_NUMBER_AHEAD         error = NewPermanentError("transaction number mismatch, the node is ahead")
	ERR_TRX_NUMBER_BEHIND        error = NewPermanentError("transaction number mismatch, the node is behind")
	ERR_TRX_PENDING              error = NewTransientError("transaction is pending")
	ERR_UNMARSHAL_TRX            error = NewTransientError("failed to unmarshal transaction")
	ERR_UPDATE_SEGMENT_INVENTORY error = NewTransientError("failed to update segment inventory")
	ERR_UPDATE_SPACE_INVENTORY   error = NewTransientError("failed to update space inventory")
	ERR_WRITE_TRX                error = NewTransientError("failed to write transaction")
)

type StreamsError struct {
	Code    int
	Message string
}

func (e *StreamsError) Error() string {
	return e.Message
}

const (
	ErrCodeTransient = 1
	ErrCodePermanent = 2
)

// Error helpers
func NewTransientError(msg string) error {
	return &StreamsError{Code: ErrCodeTransient, Message: msg}
}

func NewPermanentError(msg string) error {
	return &StreamsError{Code: ErrCodePermanent, Message: msg}
}
