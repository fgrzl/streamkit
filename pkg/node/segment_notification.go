package node

import (
	"github.com/fgrzl/messaging"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
)

type SegmentNotification struct {
	StoreID       uuid.UUID          `json:"store_id"`
	SegmentStatus *api.SegmentStatus `json:"segment_status"`
}

func (obj *SegmentNotification) GetDiscriminator() string {
	return "streamkit://api/v1/segment_notification"
}

func (obj *SegmentNotification) GetRoute() messaging.Route {
	return GetSegmentNotificationRoute(obj.StoreID, obj.SegmentStatus.Space)
}

func GetSegmentNotificationRoute(storeID uuid.UUID, space string) messaging.Route {
	inboxID := uuid.NewSHA1(storeID, []byte(space))
	return messaging.NewInboxRoute("streamkit", "segment_notification", &inboxID)
}
