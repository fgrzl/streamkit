package api

import (
	"sync"

	"github.com/fgrzl/json/polymorphic"
)

var once sync.Once

func init() { EnsureRegistered() }

func EnsureRegistered() {
	once.Do(registerAll)
}

func registerAll() {
	polymorphic.RegisterType[ConsumeSegment]()
	polymorphic.RegisterType[ConsumeSpace]()
	polymorphic.RegisterType[Consume]()
	polymorphic.RegisterType[GetSegments]()
	polymorphic.RegisterType[GetSpaces]()
	polymorphic.RegisterType[GetStatus]()
	polymorphic.RegisterType[Peek]()
	polymorphic.RegisterType[Produce]()
	polymorphic.RegisterType[SegmentNotification]()
	polymorphic.RegisterType[SegmentStatus]()
	polymorphic.RegisterType[SubscribeToSegmentStatus]()
}
