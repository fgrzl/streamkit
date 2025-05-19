package azure

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/fgrzl/streamkit/internal"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/fgrzl/streams/broker"
)

const (
	minTableNameLength = 3
	maxTableNameLength = 63
)

// AzureStoreOptions configures the Azure Table Storage client.
type AzureStoreOptions struct {
	Prefix                    string
	Endpoint                  string
	UseDefaultAzureCredential bool
	SharedKeyCredential       *aztables.SharedKeyCredential
	AllowInsecureHTTP         bool // For local Azurite testing
}

// StoreFactory creates Azure-backed stores using shared credentials.
type StoreFactory struct {
	options AzureStoreOptions
	cred    azcore.TokenCredential
	once    sync.Once
	initErr error
}

// NewStoreFactory validates options and initializes credentials.
func NewStoreFactory(bus broker.Bus, options AzureStoreOptions) (*StoreFactory, error) {
	if options.Endpoint == "" {
		return nil, errors.New("azure store factory: endpoint is required")
	}
	if !options.UseDefaultAzureCredential && options.SharedKeyCredential == nil {
		return nil, errors.New("azure store factory: credential strategy is required")
	}

	f := &StoreFactory{options: options}
	f.once.Do(func() {
		f.cred, f.initErr = f.initCredential()
	})
	if f.initErr != nil {
		return nil, fmt.Errorf("azure store factory: credential initialization failed: %w", f.initErr)
	}
	return f, nil
}

// NewStore returns a new Azure-backed store scoped to a sanitized table name.
func (f *StoreFactory) NewStore(ctx context.Context, store string) (storage.Store, error) {
	if f.initErr != nil {
		return nil, fmt.Errorf("azure store factory: credentials not initialized: %w", f.initErr)
	}

	tableName := sanitizeTableName(f.options.Prefix + store)
	url := fmt.Sprintf("%s/%s", strings.TrimSuffix(f.options.Endpoint, "/"), tableName)

	clientOpts := aztables.ClientOptions{}
	var client *aztables.Client
	var err error

	if f.options.SharedKeyCredential != nil {
		// SharedKey path (used for Azurite or explicit keys)
		client, err = aztables.NewClientWithSharedKey(url, f.options.SharedKeyCredential, &clientOpts)
	} else {
		// TokenCredential path (used for DefaultAzureCredential)
		client, err = aztables.NewClient(url, f.cred, &clientOpts)
	}
	if err != nil {
		return nil, err
	}

	cache := internal.NewExpiringCache(CacheTTL, CacheCleanupInterval)
	return NewAzureStore(ctx, client, cache)
}

// initCredential initializes and returns a shared TokenCredential if needed.
func (f *StoreFactory) initCredential() (azcore.TokenCredential, error) {
	if f.options.SharedKeyCredential != nil {
		return nil, nil // Not used in this path
	}
	if f.options.UseDefaultAzureCredential {
		return azidentity.NewDefaultAzureCredential(nil)
	}
	return nil, errors.New("azure store factory: no valid credential strategy configured")
}

// sanitizeTableName ensures the table name conforms to Azure naming rules.
func sanitizeTableName(name string) string {
	if name == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(maxTableNameLength)

	// Start with a letter
	if isLetter(name[0]) {
		b.WriteByte(name[0])
	} else {
		b.WriteByte('T')
	}

	for i := 1; i < len(name); i++ {
		if isAlphanumeric(name[i]) {
			b.WriteByte(name[i])
		}
	}

	sanitized := b.String()
	for len(sanitized) < minTableNameLength {
		sanitized += "0"
	}
	if len(sanitized) > maxTableNameLength {
		sanitized = sanitized[:maxTableNameLength]
	}

	return sanitized
}

func isLetter(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func isAlphanumeric(c byte) bool {
	return isLetter(c) || (c >= '0' && c <= '9')
}
