package api

// BidiStream represents a bidirectional communication stream between client and server.
// It supports encoding and decoding messages, as well as managing the stream lifecycle.
type BidiStream interface {
	// Encode sends a message through the stream.
	Encode(m any) error
	
	// Decode receives a message from the stream.
	Decode(m any) (err error)
	
	// CloseSend signals that no more messages will be sent, optionally with an error.
	CloseSend(error) error
	
	// Close terminates the entire stream with an optional error.
	Close(error)
	
	// EndOfStreamError returns the error type used to signal end of stream.
	EndOfStreamError() error

	// Closed returns a channel that is closed when the stream is terminated.
	Closed() <-chan struct{}
}
