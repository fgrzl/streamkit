package bus

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestShouldSetGlobalScopeAndFormatWithoutIDWhenNewGlobalRoute(t *testing.T) {
	route := NewGlobalRoute("billing", "settled")

	assert.Equal(t, ScopeGlobal, route.Scope)
	assert.Equal(t, "billing", route.Area)
	assert.Equal(t, "settled", route.Name)
	assert.Nil(t, route.ID)
	assert.Equal(t, "global.billing.settled", route.String())
}

func TestShouldSetInternalScopeAndFormatWithoutIDWhenNewInternalRoute(t *testing.T) {
	route := NewInternalRoute("streams", "rebalance")

	assert.Equal(t, ScopeInternal, route.Scope)
	assert.Equal(t, "streams", route.Area)
	assert.Equal(t, "rebalance", route.Name)
	assert.Nil(t, route.ID)
	assert.Equal(t, "internal.streams.rebalance", route.String())
}

func TestShouldSetTenantScopeAndFormatWithIDWhenNewTenantRoute(t *testing.T) {
	tenantID := uuid.New()
	route := NewTenantRoute("tenant", "updated", &tenantID)

	assert.Equal(t, ScopeTenant, route.Scope)
	assert.Equal(t, "tenant", route.Area)
	assert.Equal(t, "updated", route.Name)
	assert.Equal(t, &tenantID, route.ID)
	assert.Equal(t, "tenant.tenant.updated["+tenantID.String()+"]", route.String())
}

func TestShouldSetInboxScopeAndFormatWithIDWhenNewInboxRoute(t *testing.T) {
	inboxID := uuid.New()
	route := NewInboxRoute("notifications", "queued", &inboxID)

	assert.Equal(t, ScopeInbox, route.Scope)
	assert.Equal(t, "notifications", route.Area)
	assert.Equal(t, "queued", route.Name)
	assert.Equal(t, &inboxID, route.ID)
	assert.Equal(t, "inbox.notifications.queued["+inboxID.String()+"]", route.String())
}
