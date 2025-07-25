package api

import "github.com/fgrzl/json/polymorphic"

// Routeable is a marker interface for types that can be passed to CallStream.
// It combines polymorphic serialization capabilities with routing semantics.
type Routeable interface {
	polymorphic.Polymorphic
}
