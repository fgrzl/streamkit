package node

import (
	"github.com/fgrzl/messaging"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
)

// SegmentNotification represents a notification message about segment status changes.
type SegmentNotification struct {
	StoreID       uuid.UUID          `json:"store_id"`
	SegmentStatus *api.SegmentStatus `json:"segment_status"`
}

// GetDiscriminator returns the unique message type identifier for SegmentNotification.
func (obj *SegmentNotification) GetDiscriminator() string {
	return "streamkit://api/v1/segment_notification"
}

// GetRoute returns the messaging route for this notification.
func (obj *SegmentNotification) GetRoute() messaging.Route {
	return GetSegmentNotificationRoute(obj.StoreID, obj.SegmentStatus.Space)
}

// GetSegmentNotificationRoute creates a messaging route for segment notifications
// in the specified store and space.
func GetSegmentNotificationRoute(storeID uuid.UUID, space string) messaging.Route {
	inboxID := uuid.NewSHA1(storeID, []byte(space))
	return messaging.NewInboxRoute("streamkit", "segment_notification", &inboxID)
}
