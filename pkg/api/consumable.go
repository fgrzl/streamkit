package api

import "github.com/fgrzl/json/polymorphic"

// Consumable is a marker interface for types that support polymorphic serialization
// and can enumerate the spaces they can be consumed from. Used for routing and filtering
// in streamkit APIs and message handling.
type Consumable interface {
	polymorphic.Polymorphic
	// GetSpaces returns the list of spaces this type would be consumable from
	GetSpaces() []string
}
