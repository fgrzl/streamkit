package api

import (
	"testing"

	"github.com/fgrzl/json/polymorphic/testkit"
)

func TestShouldRegisterPolymorphicTypes(t *testing.T) {
	testkit.TestPolymorphicRegistrations(t, map[string]interface{}{
		"streamkit://api/v1/consume":                     &Consume{},
		"streamkit://api/v1/consume_segment":             &ConsumeSegment{},
		"streamkit://api/v1/consume_space":               &ConsumeSpace{},
		"streamkit://api/v1/get_segments":                &GetSegments{},
		"streamkit://api/v1/get_spaces":                  &GetSpaces{},
		"streamkit://api/v1/get_status":                  &GetStatus{},
		"streamkit://api/v1/peek":                        &Peek{},
		"streamkit://api/v1/produce":                     &Produce{},
		"streamkit://api/v1/segment_notification":        &SegmentNotification{},
		"streamkit://api/v1/segment_status":              &SegmentStatus{},
		"streamkit://api/v1/subscribe_to_segment_status": &SubscribeToSegmentStatus{},
	})
}
