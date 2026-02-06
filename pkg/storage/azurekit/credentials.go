package azurekit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
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
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		imdsEndpoint: "http://169.254.169.254/metadata/identity/oauth2/token",
	}
}

// GetToken retrieves a valid access token, refreshing if necessary
func (c *ManagedIdentityCredential) GetToken(ctx context.Context) (string, error) {
	// Check if we have a cached token that's still valid
	c.mu.RLock()
	if c.token != "" && time.Now().Before(c.tokenExpiry.Add(-5*time.Minute)) {
		token := c.token
		c.mu.RUnlock()
		return token, nil
	}
	c.mu.RUnlock()

	// Need to acquire or refresh token
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if c.token != "" && time.Now().Before(c.tokenExpiry.Add(-5*time.Minute)) {
		return c.token, nil
	}

	// Build IMDS request
	query := url.Values{}
	query.Set("api-version", "2018-02-01")
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

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to call IMDS endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("IMDS returned status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp imdsTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("failed to decode IMDS response: %w", err)
	}

	// Parse expiry time (Unix timestamp as string)
	var expiresOn int64
	fmt.Sscanf(tokenResp.ExpiresOn, "%d", &expiresOn)
	c.tokenExpiry = time.Unix(expiresOn, 0)
	c.token = tokenResp.AccessToken

	return c.token, nil
}
