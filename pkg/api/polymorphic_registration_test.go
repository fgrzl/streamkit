package api_test

import (
	"reflect"
	"testing"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
)

// getAllDiscriminatorTypes returns a list of types that implement GetDiscriminator().
func TestAllDiscriminatorTypesAreRegistered(t *testing.T) {
	// Map of discriminator string to expected type instance
	var entitiesByDiscriminator = map[string]interface{}{
		"streamkit://api/v1/consume":                     &api.Consume{},
		"streamkit://api/v1/consume_segment":             &api.ConsumeSegment{},
		"streamkit://api/v1/consume_space":               &api.ConsumeSpace{},
		"streamkit://api/v1/get_spaces":                  &api.GetSpaces{},
		"streamkit://api/v1/get_segments":                &api.GetSegments{},
		"streamkit://api/v1/get_status":                  &api.GetStatus{},
		"streamkit://api/v1/peek":                        &api.Peek{},
		"streamkit://api/v1/produce":                     &api.Produce{},
		"streamkit://api/v1/segment_status":              &api.SegmentStatus{},
		"streamkit://api/v1/subscribe_to_segment_status": &api.SubscribeToSegmentStatus{},
		"streamkit://api/v1/segment_notification":        &api.SegmentNotification{},
	}

	for discriminator, expectedType := range entitiesByDiscriminator {
		t.Run(discriminator, func(t *testing.T) {
			instance, err := polymorphic.CreateInstance(discriminator)
			if err != nil {
				t.Errorf("Failed to create instance for %s: %v", discriminator, err)
				return
			}
			if reflect.TypeOf(instance) != reflect.TypeOf(expectedType) {
				t.Errorf("Unexpected type for %s: got %T, want %T", discriminator, instance, expectedType)
			}
		})
	}
}
