package azurekit

import (
	"context"
	"fmt"

	client "github.com/fgrzl/azkit/tables"
	"github.com/fgrzl/enumerators"
)

// NewAzureTableEnumerator creates a new enumerator for Azure Table Storage entities.
// Note: This enumerator is one-shot only. Once an error occurs during enumeration,
// the enumerator becomes permanently failed and will return false from MoveNext().
// Create a new enumerator if you need to retry after an error.
func NewAzureTableEnumerator(ctx context.Context, pager *client.ListEntitiesPager) enumerators.Enumerator[*client.Entity] {
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
	pager    *client.ListEntitiesPager
	ctx      context.Context
	current  *client.Entity
	index    int
	entities []client.Entity
	err      error
}

// Current returns the current entity in the enumeration
func (a *AzureTableEnumerator) Current() (*client.Entity, error) {
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

	// Get current entity
	// Note: Azure Table Storage only base64-encodes Edm.Binary type fields.
	// We use odata=nometadata so we don't have type information.
	// The Value field comes as-is from Azure (base64 only for binary columns).
	// Our data model expects []byte which JSON unmarshals correctly.
	a.current = &a.entities[a.index]
	return true
}
