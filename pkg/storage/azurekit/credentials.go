package azurekit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const (
	// TokenRefreshBuffer is the time before expiry when we refresh the token
	TokenRefreshBuffer = 5 * time.Minute
	// IMDSRequestTimeout is the timeout for IMDS requests
	IMDSRequestTimeout = 10 * time.Second
	// IMDSAPIVersion is the API version for Azure IMDS
	IMDSAPIVersion = "2018-02-01"
)

// SharedKeyCredential holds Azure Table Storage account credentials
// This is a lightweight replacement for the Azure SDK type
type SharedKeyCredential struct {
	AccountName string
	AccountKey  string
}

// NewSharedKeyCredential creates a new shared key credential
func NewSharedKeyCredential(accountName, accountKey string) (*SharedKeyCredential, error) {
	return &SharedKeyCredential{
		AccountName: accountName,
		AccountKey:  accountKey,
	}, nil
}

// ManagedIdentityCredential acquires tokens from Azure IMDS endpoint
// This provides DefaultCredential-like behavior for Azure Container Apps and VMs
type ManagedIdentityCredential struct {
	clientID     string
	token        string
	tokenExpiry  time.Time
	mu           sync.RWMutex
	httpClient   *http.Client
	imdsEndpoint string
}

// imdsTokenResponse represents the response from Azure IMDS
type imdsTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   string `json:"expires_in"`
	ExpiresOn   string `json:"expires_on"`
	Resource    string `json:"resource"`
	TokenType   string `json:"token_type"`
	ClientID    string `json:"client_id"`
}

// NewManagedIdentityCredential creates a credential that uses Azure Managed Identity
func NewManagedIdentityCredential(clientID string) *ManagedIdentityCredential {
	return &ManagedIdentityCredential{
		clientID:     clientID,
		httpClient:   &http.Client{Timeout: IMDSRequestTimeout},
		imdsEndpoint: "http://169.254.169.254/metadata/identity/oauth2/token",
	}
}

// GetToken retrieves a valid access token, refreshing if necessary
func (c *ManagedIdentityCredential) GetToken(ctx context.Context) (string, error) {
	// Check if we have a cached token that's still valid (fast path with read lock)
	c.mu.RLock()
	cachedToken := c.token
	cachedExpiry := c.tokenExpiry
	c.mu.RUnlock()

	// Check validity outside the lock to avoid race condition
	if cachedToken != "" && time.Now().Before(cachedExpiry.Add(-TokenRefreshBuffer)) {
		return cachedToken, nil
	}

	// Need to acquire or refresh token
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have refreshed)
	if c.token != "" && time.Now().Before(c.tokenExpiry.Add(-TokenRefreshBuffer)) {
		return c.token, nil
	}

	// Build IMDS request
	query := url.Values{}
	query.Set("api-version", IMDSAPIVersion)
	query.Set("resource", "https://storage.azure.com/")
	if c.clientID != "" {
		query.Set("client_id", c.clientID)
	}

	reqURL := fmt.Sprintf("%s?%s", c.imdsEndpoint, query.Encode())
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create IMDS request: %w", err)
	}

	req.Header.Set("Metadata", "true")

	slog.Debug("requesting managed identity token", "client_id", c.clientID)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		slog.Warn("failed to call IMDS endpoint", "error", err)
		return "", fmt.Errorf("failed to call IMDS endpoint: %w", err)
	}
	defer resp.Body.Close()

	// Extract request ID for debugging
	requestID := resp.Header.Get("x-ms-request-id")

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Error("IMDS returned error",
			"status", resp.StatusCode,
			"request_id", requestID,
			"body", string(body))
		return "", fmt.Errorf("IMDS returned status %d (request_id=%s): %s", resp.StatusCode, requestID, string(body))
	}

	var tokenResp imdsTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		slog.Error("failed to decode IMDS response", "error", err, "request_id", requestID)
		return "", fmt.Errorf("failed to decode IMDS response: %w", err)
	}

	// Parse expiry time (Unix timestamp as string) - use strconv for better performance
	expiresOn, err := strconv.ParseInt(tokenResp.ExpiresOn, 10, 64)
	if err != nil {
		slog.Error("failed to parse token expiry", "error", err, "expires_on", tokenResp.ExpiresOn)
		return "", fmt.Errorf("failed to parse token expiry: %w", err)
	}

	c.tokenExpiry = time.Unix(expiresOn, 0)
	c.token = tokenResp.AccessToken

	slog.Debug("successfully refreshed managed identity token",
		"expires_at", c.tokenExpiry,
		"request_id", requestID)

	return c.token, nil
}
