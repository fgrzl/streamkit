package bus

import (
	"fmt"

	"github.com/google/uuid"
)

// Scope defines the visibility and access control level of a message route.
type Scope string

const (
	ScopeGlobal   Scope = "global"
	ScopeInternal Scope = "internal"
	ScopeTenant   Scope = "tenant"
	ScopeInbox    Scope = "inbox"
)

// Route defines how a message is routed within the system.
type Route struct {
	Scope Scope
	Area  string
	Name  string
	ID    *uuid.UUID
}

func NewGlobalRoute(area, name string) Route {
	return Route{Scope: ScopeGlobal, Area: area, Name: name}
}

func NewInternalRoute(area, name string) Route {
	return Route{Scope: ScopeInternal, Area: area, Name: name}
}

func NewTenantRoute(area, name string, tenantID *uuid.UUID) Route {
	return Route{Scope: ScopeTenant, Area: area, Name: name, ID: tenantID}
}

func NewInboxRoute(area, name string, inboxID *uuid.UUID) Route {
	return Route{Scope: ScopeInbox, Area: area, Name: name, ID: inboxID}
}

func (r Route) String() string {
	if r.ID != nil {
		return fmt.Sprintf("%s.%s.%s[%s]", r.Scope, r.Area, r.Name, r.ID)
	}
	return fmt.Sprintf("%s.%s.%s", r.Scope, r.Area, r.Name)
}
