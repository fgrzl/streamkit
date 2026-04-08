package wskit

import (
	"testing"

	"github.com/fgrzl/claims"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldAllowAllForClientMuxerSession(t *testing.T) {
	// Arrange
	s := NewClientMuxerSession()

	// Act / Assert
	require.NotNil(t, s)
	assert.True(t, s.AllowAllStores())
	// AllowedStores returns nil when allowAll is true
	assert.Nil(t, s.AllowedStores())
	// any store should be allowed
	assert.True(t, s.CanAccessStore(uuid.New()))
}

func TestShouldAllowAllWhenServerSessionHasAllScope(t *testing.T) {
	// Arrange
	sp := claims.SerializablePrincipal{Scopes: []string{ScopeAllStores}}
	p := claims.NewReconstructedPrincipal(sp)

	// Act
	s, err := NewServerMuxerSession(p)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.True(t, s.AllowAllStores())
}

func TestShouldAllowAccessForSpecificStore(t *testing.T) {
	// Arrange
	id := uuid.New()
	scope := ScopePrefix + id.String()
	sp := claims.SerializablePrincipal{Scopes: []string{scope}}
	p := claims.NewReconstructedPrincipal(sp)

	// Act
	s, err := NewServerMuxerSession(p)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.False(t, s.AllowAllStores())

	// AllowedStores should contain the id
	stores := s.AllowedStores()
	require.Len(t, stores, 1)
	assert.Equal(t, id, stores[0])

	// CanAccessStore true for id, false for others
	assert.True(t, s.CanAccessStore(id))
	assert.False(t, s.CanAccessStore(uuid.New()))
}

func TestShouldErrorForInvalidOrEmptyScopes(t *testing.T) {
	// Arrange / Act: No scopes
	sp := claims.SerializablePrincipal{Scopes: []string{}}
	p := claims.NewReconstructedPrincipal(sp)
	s, err := NewServerMuxerSession(p)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, s)

	// Arrange / Act: Invalid uuid in scope
	sp2 := claims.SerializablePrincipal{Scopes: []string{ScopePrefix + "not-a-uuid"}}
	p2 := claims.NewReconstructedPrincipal(sp2)
	s2, err2 := NewServerMuxerSession(p2)

	// Assert
	assert.Error(t, err2)
	assert.Nil(t, s2)
}

func TestShouldRejectTokenWithMixedValidAndMalformedScopes(t *testing.T) {
	// Arrange: one valid per-store scope and one malformed one
	validID := uuid.New()
	sp := claims.SerializablePrincipal{Scopes: []string{
		ScopePrefix + validID.String(),
		ScopePrefix + "not-a-uuid",
	}}
	p := claims.NewReconstructedPrincipal(sp)

	// Act
	s, err := NewServerMuxerSession(p)

	// Assert: a malformed scope must fail the whole request, not grant partial access
	assert.Error(t, err)
	assert.Nil(t, s)
}
