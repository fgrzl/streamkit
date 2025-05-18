package azure

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/fgrzl/enumerators"
)

// NewAzureTableEnumerator creates a new enumerator for Azure Table Storage entities
func NewAzureTableEnumerator(ctx context.Context, pager *runtime.Pager[aztables.ListEntitiesResponse]) enumerators.Enumerator[*Entity] {
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
	pager    *runtime.Pager[aztables.ListEntitiesResponse]
	ctx      context.Context
	current  *Entity
	index    int
	entities [][]byte
	err      error
}

// Current returns the current entity in the enumeration
func (a *AzureTableEnumerator) Current() (*Entity, error) {
	if a.err != nil {
		return nil, a.err
	}
	if a.current == nil || a.index < 0 || a.index >= len(a.entities) {
		return nil, fmt.Errorf("no current entity available")
	}
	return a.current, nil
}

// Dispose cleans up resources (no-op for this implementation as the pager manages its own resources)
func (a *AzureTableEnumerator) Dispose() {
	// The runtime.Pager handles its own cleanup, so we don't need to do anything here
	// Setting fields to nil to help garbage collection
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

	// Move to next entity in current page
	a.index++
	if a.entities != nil && a.index < len(a.entities) {
		if err := a.setCurrent(); err != nil {
			a.err = err
			return false
		}
		return true
	}

	// Check if there are more pages
	if !a.pager.More() {
		return false
	}

	// Fetch next page
	resp, err := a.pager.NextPage(a.ctx)
	if err != nil {
		a.err = fmt.Errorf("failed to fetch next page: %w", err)
		return false
	}

	a.entities = resp.Entities
	a.index = 0

	if len(a.entities) == 0 {
		return a.MoveNext() // Recurse if page is empty
	}

	if err := a.setCurrent(); err != nil {
		a.err = err
		return false
	}
	return true
}

// setCurrent unmarshals the current entity from the Azure response
func (a *AzureTableEnumerator) setCurrent() error {
	if a.index < 0 || a.index >= len(a.entities) {
		return fmt.Errorf("index out of bounds")
	}

	var entity Entity
	if err := json.Unmarshal(a.entities[a.index], &entity); err != nil {
		return fmt.Errorf("failed to unmarshal entity: %w", err)
	}
	a.current = &entity
	return nil
}
