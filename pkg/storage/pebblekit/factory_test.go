package pebblekit

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStoreFactoryShouldKeepProvidedOptions(t *testing.T) {
	options := &PebbleStoreOptions{Path: t.TempDir()}

	factory, err := NewStoreFactory(options)

	require.NoError(t, err)
	require.NotNil(t, factory)
	assert.Same(t, options, factory.options)
}

func TestStoreFactoryShouldCreateStoreInStoreScopedDirectory(t *testing.T) {
	options := &PebbleStoreOptions{Path: t.TempDir()}
	factory, err := NewStoreFactory(options)
	require.NoError(t, err)

	storeID := uuid.New()
	store, err := factory.NewStore(context.Background(), storeID)
	require.NoError(t, err)
	t.Cleanup(func() {
		store.Close()
	})

	pebbleStore, ok := store.(*PebbleStore)
	require.True(t, ok)
	assert.NotNil(t, pebbleStore.db)
	assert.NotNil(t, pebbleStore.cache)
	assert.DirExists(t, filepath.Join(options.Path, storeID.String(), "streams"))
}

func TestStoreFactoryShouldCreateDistinctDirectoriesPerStoreID(t *testing.T) {
	options := &PebbleStoreOptions{Path: t.TempDir()}
	factory, err := NewStoreFactory(options)
	require.NoError(t, err)

	firstStoreID := uuid.New()
	secondStoreID := uuid.New()

	firstStore, err := factory.NewStore(context.Background(), firstStoreID)
	require.NoError(t, err)
	t.Cleanup(func() {
		firstStore.Close()
	})

	secondStore, err := factory.NewStore(context.Background(), secondStoreID)
	require.NoError(t, err)
	t.Cleanup(func() {
		secondStore.Close()
	})

	firstPath := filepath.Join(options.Path, firstStoreID.String(), "streams")
	secondPath := filepath.Join(options.Path, secondStoreID.String(), "streams")

	assert.NotEqual(t, firstPath, secondPath)
	assert.DirExists(t, firstPath)
	assert.DirExists(t, secondPath)
}
