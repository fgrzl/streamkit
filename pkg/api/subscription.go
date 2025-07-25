package api

// Subscription represents an active subscription that can be cancelled.
type Subscription interface {
	// Unsubscribe cancels the subscription and releases associated resources.
	Unsubscribe()
}
