// Package azurekit provides an Azure Table Storage-based implementation
// for the streamkit streaming platform.
//
// This package implements the storage interface using Azure Table Storage
// as the backend, providing cloud-native, scalable storage with support
// for distributed access and high availability.
package azurekit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/fgrzl/streamkit/internal/cache"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
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
	options *AzureStoreOptions
	cred    azcore.TokenCredential
	once    sync.Once
	initErr error
}

// NewStoreFactory validates options and initializes credentials.
func NewStoreFactory(options *AzureStoreOptions) (*StoreFactory, error) {
	if options.Endpoint == "" {
		return nil, errors.New("azure store factory: endpoint is required")
	}
	if !options.UseDefaultAzureCredential && options.SharedKeyCredential == nil {
		return nil, errors.New("azure store factory: credential strategy is required")
	}

	// Helpful warning: HTTP endpoint with DefaultAzureCredential usually indicates Azurite;
	// AAD tokens won't work against Azurite. Prefer SharedKey in that case.
	if strings.HasPrefix(strings.ToLower(options.Endpoint), "http://") && options.UseDefaultAzureCredential {
		slog.WarnContext(context.Background(), "azure store factory: HTTP endpoint with DefaultAzureCredential detected; this likely requires SharedKey (Azurite)",
			slog.String("endpoint", options.Endpoint))
	}

	f := &StoreFactory{options: options}
	f.once.Do(func() {
		f.cred, f.initErr = f.initCredential()
	})
	if f.initErr != nil {
		slog.ErrorContext(context.Background(), "azure store factory: credential initialization failed", slog.String("err", f.initErr.Error()))
		return nil, fmt.Errorf("azure store factory: credential initialization failed: %w", f.initErr)
	}
	slog.InfoContext(context.Background(), "azure store factory: initialized",
		slog.String("endpoint", options.Endpoint),
		slog.Bool("use_default_credential", options.UseDefaultAzureCredential),
		slog.Bool("shared_key_provided", options.SharedKeyCredential != nil),
	)
	return f, nil
}

// NewStore returns a new Azure-backed store scoped to a sanitized table name.
func (f *StoreFactory) NewStore(ctx context.Context, storeID uuid.UUID) (storage.Store, error) {
	if f.initErr != nil {
		return nil, fmt.Errorf("azure store factory: credentials not initialized: %w", f.initErr)
	}

	tableName := sanitizeTableName(f.options.Prefix + storeID.String())
	url := fmt.Sprintf("%s/%s", strings.TrimSuffix(f.options.Endpoint, "/"), tableName)

	slog.DebugContext(ctx, "azure store: creating client",
		slog.String("url", url),
		slog.String("table", tableName),
		slog.Bool("aad", f.options.SharedKeyCredential == nil),
	)

	// Optimize HTTP transport for high-throughput scenarios
	// Azure SDK uses default http.Client, but we can configure for better performance
	clientOpts := aztables.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: nil, // SDK will use default http.DefaultTransport
			// Note: SDK configures reasonable defaults for retry policy
			// We handle application-level retries in processChunkWithRetry
		},
	}
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
		slog.ErrorContext(ctx, "azure store: failed to create client", slog.String("url", url), slog.String("err", err.Error()))
		return nil, err
	}

	cache := cache.NewExpiringCache(CacheTTL, CacheCleanupInterval)
	return NewAzureStore(ctx, client, cache)
}

// initCredential initializes and returns a shared TokenCredential if needed.
func (f *StoreFactory) initCredential() (azcore.TokenCredential, error) {
	if f.options.SharedKeyCredential != nil {
		slog.InfoContext(context.Background(), "azure store factory: using SharedKey credential")
		return nil, nil // Not used in this path
	}
	if f.options.UseDefaultAzureCredential {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			slog.ErrorContext(context.Background(), "azure store factory: DefaultAzureCredential creation failed", slog.String("err", err.Error()))
			return nil, err
		}
		slog.InfoContext(context.Background(), "azure store factory: using DefaultAzureCredential")
		return cred, nil
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
