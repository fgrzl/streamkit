package client

import (
	"testing"
	"time"
)

func TestNewOTelClientMetrics_RecordsWithoutPanic(t *testing.T) {
	m := NewOTelClientMetrics()
	if m == nil {
		t.Fatal("NewOTelClientMetrics returned nil")
	}
	// RecordSubscriptionReplay includes the new replay duration histogram and low-cardinality attrs.
	m.RecordSubscriptionReplay("sub-id", true, 10*time.Millisecond)
	m.RecordSubscriptionReplay("sub-id", false, 100*time.Millisecond)
	m.RecordProduceLatency("space", "segment", 5*time.Millisecond)
	m.RecordConsumeLatency("space", "segment", 2*time.Millisecond)
	m.RecordHandlerTimeout("sub-id")
	m.RecordHandlerPanic("sub-id")
}
