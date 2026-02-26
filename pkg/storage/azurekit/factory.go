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
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/fgrzl/azkit/credentials"
	client "github.com/fgrzl/azkit/tables"
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
	Prefix            string
	AccountName       string // Azure storage account name
	AccountKey        string // Azure storage account key (leave empty to use Managed Identity)
	Endpoint          string // Custom endpoint (e.g., http://127.0.0.1:10002/devstoreaccount1 for Azurite)
	AllowInsecureHTTP bool   // For local Azurite testing

	// Managed Identity options
	UseManagedIdentity bool   // Use Azure Managed Identity instead of SharedKey
	ManagedIdentityID  string // Optional: specific managed identity client ID

	// Optional: use an existing HTTP client (useful for tests)
	HTTPClient *client.HTTPTableClient

	// Runtime knobs
	AddWorkers                 int
	BatchSize                  int
	MaxTransactionPayloadBytes int
	CacheTTL                   int // seconds, zero means use default
	CacheCleanupInterval       int // seconds, zero means use default

	// Optional override table name (useful for tests)
	TableNameOverride string

	// SkipTableCreation skips the create-table-if-not-exists call (set by factory when table already ensured in this process).
	SkipTableCreation bool
}

// StoreFactory creates Azure-backed stores using shared credentials.
type StoreFactory struct {
	options       *AzureStoreOptions
	ensuredTables sync.Map // table name -> struct{}; tables we've already ensured in this process
}

// NewStoreFactory validates options.
func NewStoreFactory(ctx context.Context, options *AzureStoreOptions) (*StoreFactory, error) {
	if options.AccountName == "" && (options.HTTPClient == nil) {
		return nil, errors.New("azure store factory: account name is required or HTTPClient must be provided")
	}
	if !options.UseManagedIdentity && options.AccountKey == "" && options.HTTPClient == nil {
		return nil, errors.New("azure store factory: account key is required (or use UseManagedIdentity=true) or HTTPClient must be provided")
	}

	authMethod := "SharedKey"
	if options.UseManagedIdentity {
		authMethod = "ManagedIdentity"
	}

	slog.InfoContext(ctx, "azure store factory: initialized",
		slog.String("account", options.AccountName),
		slog.String("auth", authMethod),
		slog.Bool("allow_insecure", options.AllowInsecureHTTP),
	)
	return &StoreFactory{options: options}, nil
}

// NewStore returns a new Azure-backed store scoped to a sanitized table name.
func (f *StoreFactory) NewStore(ctx context.Context, storeID uuid.UUID) (storage.Store, error) {
	// Allow explicit override for table name (useful for tests)
	var tableName string
	if f.options.TableNameOverride != "" {
		tableName = sanitizeTableName(f.options.TableNameOverride)
	} else {
		tableName = sanitizeTableName(f.options.Prefix + storeID.String())
	}

	var httpClient *client.HTTPTableClient
	if f.options.HTTPClient != nil {
		httpClient = f.options.HTTPClient
	} else {
		var err error
		if f.options.UseManagedIdentity {
			managedCred := credentials.NewManagedIdentityCredential(f.options.ManagedIdentityID)
			httpClient, err = client.NewHTTPTableClientWithManagedIdentity(f.options.AccountName, managedCred, tableName, f.options.AllowInsecureHTTP, f.options.Endpoint)
		} else {
			httpClient, err = client.NewHTTPTableClient(f.options.AccountName, f.options.AccountKey, tableName, f.options.AllowInsecureHTTP, f.options.Endpoint)
		}
		if err != nil {
			slog.ErrorContext(ctx, "azure store: failed to create HTTP client", slog.String("err", err.Error()))
			return nil, err
		}
	}

	// Use configured cache TTL/cleanup if provided
	cacheTTL := CacheTTL
	cacheCleanup := CacheCleanupInterval
	if f.options.CacheTTL > 0 {
		cacheTTL = time.Duration(f.options.CacheTTL) * time.Second
	}
	if f.options.CacheCleanupInterval > 0 {
		cacheCleanup = time.Duration(f.options.CacheCleanupInterval) * time.Second
	}
	cacheObj := cache.NewExpiringCache(cacheTTL, cacheCleanup)

	opts := *f.options
	_, alreadyEnsured := f.ensuredTables.Load(tableName)
	if alreadyEnsured {
		opts.SkipTableCreation = true
	}

	store, err := NewAzureStore(ctx, httpClient, cacheObj, &opts)
	if err != nil {
		return nil, err
	}
	if !alreadyEnsured {
		f.ensuredTables.Store(tableName, struct{}{})
	}
	return store, nil
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
