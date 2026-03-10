package api

// Lease represents a held lease that is renewed in the background until Release() is called.
// Done() is closed when Release() has completed. Release() is idempotent.
type Lease interface {
	Release() error
	Done() <-chan struct{}
}
