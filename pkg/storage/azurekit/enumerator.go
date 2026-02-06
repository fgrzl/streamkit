package azurekit

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/fgrzl/enumerators"
)

// NewAzureTableEnumerator creates a new enumerator for Azure Table Storage entities
func NewAzureTableEnumerator(ctx context.Context, pager *ListEntitiesPager) enumerators.Enumerator[*entity] {
	return &AzureTableEnumerator{
		pager:    pager,
		ctx:      ctx,
		current:  nil,
		index:    -1,
		entities: nil,
		err:      nil,
	}
}

type AzureTableEnumerator struct {
	pager    *ListEntitiesPager
	ctx      context.Context
	current  *entity
	index    int
	entities []entity
	err      error
}

// Current returns the current entity in the enumeration
func (a *AzureTableEnumerator) Current() (*entity, error) {
	if a.err != nil {
		return nil, a.err
	}
	if a.current == nil || a.index < 0 || a.index >= len(a.entities) {
		return nil, fmt.Errorf("no current entity available")
	}
	return a.current, nil
}

// Dispose cleans up resources
func (a *AzureTableEnumerator) Dispose() {
	a.pager = nil
	a.current = nil
	a.entities = nil
	a.err = nil
}

// Err returns any error that occurred during enumeration
func (a *AzureTableEnumerator) Err() error {
	return a.err
}

// MoveNext advances to the next entity in the enumeration
func (a *AzureTableEnumerator) MoveNext() bool {
	if a.err != nil {
		return false
	}

	a.index++

	// Load next page if needed
	if a.entities == nil || a.index >= len(a.entities) {
		page, err := a.pager.FetchPage(a.ctx)
		if err != nil {
			a.err = err
			return false
		}
		if len(page) == 0 {
			return false
		}
		a.entities = page
		a.index = 0
	}

	if a.index >= len(a.entities) {
		return false
	}

	// Get current entity and decode Value if needed
	e := &a.entities[a.index]

	// Decode base64-encoded Value field
	if len(e.Value) > 0 {
		// Azure Table Storage returns binary data as base64-encoded strings
		// Try to decode it
		str := string(e.Value)
		decoded, err := base64.StdEncoding.DecodeString(str)
		if err == nil {
			e.Value = decoded
		}
	}

	a.current = e
	return true
}
