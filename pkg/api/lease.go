package api

import "context"

// Lease represents a held lease that is renewed in the background until Release() is called.
// Done() is closed when Release() has completed. Release() is idempotent.
// Context() returns a context that is canceled when the lease is released or lost (e.g. renew failed).
type Lease interface {
	Release() error
	Done() <-chan struct{}
	Context() context.Context
}
