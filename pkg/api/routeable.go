package api

import "github.com/fgrzl/json/polymorphic"

// Marker interface for types that can be passed to CallStream
type Routeable interface {
	polymorphic.Polymorphic
}
