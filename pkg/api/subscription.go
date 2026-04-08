package api

// Subscription represents an active subscription that can be canceled.
type Subscription interface {
	// ID returns the stable identifier used by GetSubscriptionStatus.
	ID() string

	// Unsubscribe cancels the subscription and releases associated resources.
	Unsubscribe()
}
