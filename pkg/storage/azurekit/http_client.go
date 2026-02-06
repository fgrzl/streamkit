package azurekit

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	// bufferPool for reusing byte buffers
	bufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}

	// builderPool for reusing strings.Builder
	builderPool = sync.Pool{
		New: func() interface{} {
			return new(strings.Builder)
		},
	}
)

// HTTPTableClient provides a lightweight native Go HTTP client for Azure Table Storage REST API
type HTTPTableClient struct {
	endpoint       string
	tableName      string
	accountName    string
	accountKey     string
	sasToken       string                     // SAS token for auth (alternative to SharedKey)
	managedCred    *ManagedIdentityCredential // For Bearer token auth
	httpClient     *http.Client
	allowInsecure  bool
	useBearerToken bool // true if using Managed Identity
	useSAS         bool // true if using SAS token
}

// NewHTTPTableClient creates a new HTTP-based Table Storage client with SharedKey authentication
func NewHTTPTableClient(accountName, accountKey, tableName string, allowInsecure bool, customEndpoint string) (*HTTPTableClient, error) {
	if accountName == "" || accountKey == "" {
		return nil, fmt.Errorf("account name and key are required")
	}

	var endpoint string
	if customEndpoint != "" {
		endpoint = customEndpoint
	} else {
		endpoint = fmt.Sprintf("https://%s.table.core.windows.net", accountName)
		if allowInsecure {
			endpoint = fmt.Sprintf("http://%s.table.core.windows.net", accountName)
		}
	}

	return &HTTPTableClient{
		endpoint:       endpoint,
		tableName:      tableName,
		accountName:    accountName,
		accountKey:     accountKey,
		httpClient:     newOptimizedHTTPClient(),
		allowInsecure:  allowInsecure,
		useBearerToken: false,
		useSAS:         false,
	}, nil
}

// NewHTTPTableClientWithSAS creates a client with SAS token authentication
func NewHTTPTableClientWithSAS(accountName, sasToken, tableName string, allowInsecure bool, customEndpoint string) (*HTTPTableClient, error) {
	if accountName == "" || sasToken == "" {
		return nil, fmt.Errorf("account name and SAS token are required")
	}

	var endpoint string
	if customEndpoint != "" {
		endpoint = customEndpoint
	} else {
		endpoint = fmt.Sprintf("https://%s.table.core.windows.net", accountName)
		if allowInsecure {
			endpoint = fmt.Sprintf("http://%s.table.core.windows.net", accountName)
		}
	}

	return &HTTPTableClient{
		endpoint:       endpoint,
		tableName:      tableName,
		accountName:    accountName,
		sasToken:       sasToken,
		httpClient:     newOptimizedHTTPClient(),
		allowInsecure:  allowInsecure,
		useBearerToken: false,
		useSAS:         true,
	}, nil
}

// NewHTTPTableClientWithManagedIdentity creates a client with Managed Identity (Bearer token) authentication
func NewHTTPTableClientWithManagedIdentity(accountName string, managedCred *ManagedIdentityCredential, tableName string, allowInsecure bool, customEndpoint string) (*HTTPTableClient, error) {
	if accountName == "" {
		return nil, fmt.Errorf("account name is required")
	}
	if managedCred == nil {
		return nil, fmt.Errorf("managed identity credential is required")
	}

	var endpoint string
	if customEndpoint != "" {
		endpoint = customEndpoint
	} else {
		endpoint = fmt.Sprintf("https://%s.table.core.windows.net", accountName)
		if allowInsecure {
			endpoint = fmt.Sprintf("http://%s.table.core.windows.net", accountName)
		}
	}

	return &HTTPTableClient{
		endpoint:       endpoint,
		tableName:      tableName,
		accountName:    accountName,
		managedCred:    managedCred,
		httpClient:     newOptimizedHTTPClient(),
		allowInsecure:  allowInsecure,
		useBearerToken: true,
		useSAS:         false,
	}, nil
}

// newOptimizedHTTPClient creates an HTTP client optimized for high throughput
func newOptimizedHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		},
	}
}

// Entity represents a table storage entity with metadata
type entity struct {
	PartitionKey string `json:"PartitionKey"`
	RowKey       string `json:"RowKey"`
	Value        []byte `json:"Value,omitempty"`
	Timestamp    string `json:"Timestamp,omitempty"`
}

// CreateTable ensures the table exists
func (c *HTTPTableClient) CreateTable(ctx context.Context) error {
	body := map[string]string{"TableName": c.tableName}
	data, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint+"/Tables", bytes.NewReader(data))
	if err != nil {
		return err
	}

	c.signRequest(req, "POST", []byte{}, "/Tables")
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Table already exists is acceptable (409)
	if resp.StatusCode == http.StatusConflict {
		return nil
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		var respBody []byte
		respBody, _ = io.ReadAll(resp.Body)
		return fmt.Errorf("create table failed: status=%d body=%s", resp.StatusCode, string(respBody))
	}

	return nil
}

// AddEntity inserts a new entity (insert-only semantics)
func (c *HTTPTableClient) AddEntity(ctx context.Context, data []byte) error {
	var e entity
	if err := json.Unmarshal(data, &e); err != nil {
		return err
	}

	reqBody := map[string]interface{}{
		"PartitionKey": e.PartitionKey,
		"RowKey":       e.RowKey,
	}
	if len(e.Value) > 0 {
		reqBody["Value"] = base64.StdEncoding.EncodeToString(e.Value)
	}

	body := bufferPool.Get().(*bytes.Buffer)
	body.Reset()
	defer bufferPool.Put(body)

	enc := json.NewEncoder(body)
	if err := enc.Encode(reqBody); err != nil {
		return err
	}

	// Use odata=nometadata for minimal response
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	b.WriteString(c.endpoint)
	b.WriteByte('/')
	b.WriteString(c.tableName)
	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, bytes.NewReader(body.Bytes()))
	if err != nil {
		return err
	}

	c.signRequest(req, "POST", body.Bytes(), fmt.Sprintf("/%s", c.tableName))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add entity failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	return nil
}

// AddEntityBatch inserts multiple entities in a single batch request (up to 100)
// Uses multipart/mixed format for efficient bulk inserts
func (c *HTTPTableClient) AddEntityBatch(ctx context.Context, entities [][]byte) error {
	if len(entities) == 0 {
		return nil
	}
	if len(entities) > 100 {
		return fmt.Errorf("batch size exceeds maximum of 100 entities")
	}

	// Generate batch and changeset boundaries
	batchID := fmt.Sprintf("batch_%d", time.Now().UnixNano())
	changesetID := fmt.Sprintf("changeset_%d", time.Now().UnixNano())

	body := bufferPool.Get().(*bytes.Buffer)
	body.Reset()
	defer bufferPool.Put(body)

	// Build multipart/mixed request
	fmt.Fprintf(body, "--%s\r\n", batchID)
	fmt.Fprintf(body, "Content-Type: multipart/mixed; boundary=%s\r\n\r\n", changesetID)

	for i, entData := range entities {
		var e entity
		if err := json.Unmarshal(entData, &e); err != nil {
			return fmt.Errorf("failed to unmarshal entity %d: %w", i, err)
		}

		reqBody := map[string]interface{}{
			"PartitionKey": e.PartitionKey,
			"RowKey":       e.RowKey,
		}
		if len(e.Value) > 0 {
			reqBody["Value"] = base64.StdEncoding.EncodeToString(e.Value)
		}

		entJSON, _ := json.Marshal(reqBody)

		fmt.Fprintf(body, "--%s\r\n", changesetID)
		fmt.Fprintf(body, "Content-Type: application/http\r\n")
		fmt.Fprintf(body, "Content-Transfer-Encoding: binary\r\n\r\n")
		fmt.Fprintf(body, "POST %s/%s HTTP/1.1\r\n", c.endpoint, c.tableName)
		fmt.Fprintf(body, "Content-Type: application/json\r\n")
		fmt.Fprintf(body, "Accept: application/json;odata=nometadata\r\n")
		fmt.Fprintf(body, "Content-Length: %d\r\n\r\n", len(entJSON))
		body.Write(entJSON)
		body.WriteString("\r\n")
	}

	fmt.Fprintf(body, "--%s--\r\n", changesetID)
	fmt.Fprintf(body, "--%s--\r\n", batchID)

	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	b.WriteString(c.endpoint)
	b.WriteString("/$batch")
	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, bytes.NewReader(body.Bytes()))
	if err != nil {
		return err
	}

	c.signRequest(req, "POST", body.Bytes(), "/$batch")
	req.Header.Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", batchID))
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("batch failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	return nil
}

// UpsertEntity updates or inserts an entity (merge semantics)
func (c *HTTPTableClient) UpsertEntity(ctx context.Context, data []byte, mode string) error {
	var e entity
	if err := json.Unmarshal(data, &e); err != nil {
		return err
	}

	reqBody := map[string]interface{}{
		"PartitionKey": e.PartitionKey,
		"RowKey":       e.RowKey,
	}
	if len(e.Value) > 0 {
		reqBody["Value"] = base64.StdEncoding.EncodeToString(e.Value)
	}

	body, _ := json.Marshal(reqBody)

	reqURL := fmt.Sprintf("%s/%s(PartitionKey='%s',RowKey='%s')",
		c.endpoint, c.tableName, url.QueryEscape(e.PartitionKey), url.QueryEscape(e.RowKey))

	method := "PATCH" // Merge (update if exists, insert if not)
	if mode == "Replace" {
		method = "PUT" // Replace (overwrite if exists, insert if not)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, bytes.NewReader(body))
	if err != nil {
		return err
	}

	c.signRequest(req, method, body, fmt.Sprintf("/%s(PartitionKey='%s',RowKey='%s')",
		c.tableName, url.QueryEscape(e.PartitionKey), url.QueryEscape(e.RowKey)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upsert entity failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteEntity removes an entity
func (c *HTTPTableClient) DeleteEntity(ctx context.Context, pk, rk string) error {
	reqURL := fmt.Sprintf("%s/%s(PartitionKey='%s',RowKey='%s')",
		c.endpoint, c.tableName, url.QueryEscape(pk), url.QueryEscape(rk))

	req, err := http.NewRequestWithContext(ctx, "DELETE", reqURL, nil)
	if err != nil {
		return err
	}

	c.signRequest(req, "DELETE", nil, fmt.Sprintf("/%s(PartitionKey='%s',RowKey='%s')",
		c.tableName, url.QueryEscape(pk), url.QueryEscape(rk)))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 404 is acceptable (entity doesn't exist)
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete entity failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	return nil
}

// GetEntity retrieves a single entity
func (c *HTTPTableClient) GetEntity(ctx context.Context, pk, rk string) ([]byte, error) {
	reqURL := fmt.Sprintf("%s/%s(PartitionKey='%s',RowKey='%s')?$format=json",
		c.endpoint, c.tableName, url.QueryEscape(pk), url.QueryEscape(rk))

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}

	c.signRequest(req, "GET", nil, fmt.Sprintf("/%s(PartitionKey='%s',RowKey='%s')",
		c.tableName, url.QueryEscape(pk), url.QueryEscape(rk)))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("ResourceNotFound")
	}

	if resp.StatusCode != http.StatusOK {
		var respBody []byte
		respBody, _ = io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get entity failed: status=%d body=%s", resp.StatusCode, string(respBody))
	}

	var result entity
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	// Re-encode as JSON byte slice
	return json.Marshal(result)
}

// ListEntitiesPager provides pagination for list entities queries
type ListEntitiesPager struct {
	client            *HTTPTableClient
	filter            string
	selectCols        string
	top               int32
	continuationToken string
	ctx               context.Context
	currentPage       []entity
	pageIndex         int
	done              bool
}

// NewListEntitiesPager creates a new pager for listing entities
func (c *HTTPTableClient) NewListEntitiesPager(filter, selectCols string, top int32) *ListEntitiesPager {
	if top == 0 || top > 1000 {
		top = 1000
	}
	return &ListEntitiesPager{
		client:     c,
		filter:     filter,
		selectCols: selectCols,
		top:        top,
		pageIndex:  -1,
	}
}

// PageResponse represents a page response
type PageResponse struct {
	Value     []entity `json:"value"`
	NextToken string   `json:"odata.nextLink,omitempty"`
}

// FetchPage fetches the next page of results
func (p *ListEntitiesPager) FetchPage(ctx context.Context) ([]entity, error) {
	p.ctx = ctx

	// Build query string
	query := fmt.Sprintf("%s/%s?$format=json&$top=%d", p.client.endpoint, p.client.tableName, p.top)

	if p.filter != "" {
		query += "&$filter=" + url.QueryEscape(p.filter)
	}
	if p.selectCols != "" {
		query += "&$select=" + url.QueryEscape(p.selectCols)
	}
	if p.continuationToken != "" {
		query += "&" + p.continuationToken
	}

	req, err := http.NewRequestWithContext(ctx, "GET", query, nil)
	if err != nil {
		return nil, err
	}

	p.client.signRequest(req, "GET", nil, fmt.Sprintf("/%s", p.client.tableName))

	resp, err := p.client.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list entities failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var pageResp PageResponse
	if err := json.NewDecoder(resp.Body).Decode(&pageResp); err != nil {
		return nil, err
	}

	// Extract continuation token if present
	if pageResp.NextToken != "" {
		// Parse continuation token from odata.nextLink
		parts := strings.Split(pageResp.NextToken, "?")
		if len(parts) > 1 {
			p.continuationToken = parts[1]
		}
	} else {
		p.done = true
	}

	p.pageIndex++
	return pageResp.Value, nil
}

// IsDone returns whether there are more pages
func (p *ListEntitiesPager) IsDone() bool {
	return p.done
}

// Close closes the pager
func (p *ListEntitiesPager) Close() error {
	return nil
}

// signRequest signs an HTTP request with either SharedKey or Bearer token authentication
// Based on: https://docs.microsoft.com/en-us/rest/api/storageservices/authenticate-with-shared-key
func (c *HTTPTableClient) signRequest(req *http.Request, method string, body []byte, resourcePath string) {
	// Set standard headers
	req.Header.Set("x-ms-version", "2021-06-08")
	date := time.Now().UTC().Format(time.RFC1123)
	req.Header.Set("x-ms-date", date)

	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
		req.Header.Set("Content-Type", contentType)
	}

	// Use Bearer token authentication if Managed Identity is configured
	if c.useBearerToken && c.managedCred != nil {
		token, err := c.managedCred.GetToken(req.Context())
		if err != nil {
			// Fallback behavior - log error but don't fail the request
			// The request will likely fail with 401 but that's better than panicking
			fmt.Fprintf(os.Stderr, "[WARN] Failed to acquire managed identity token: %v\n", err)
			return
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		return
	}

	// Use SharedKey authentication
	// Extract and normalize x-ms-* headers for signing
	type header struct {
		name  string
		value string
	}
	var xmsHeaders []header

	// Collect x-ms-* headers but EXCLUDE x-ms-date (it goes in Date field for string to sign)
	for headerName, headerValues := range req.Header {
		lowerName := strings.ToLower(headerName)
		if strings.HasPrefix(lowerName, "x-ms-") && lowerName != "x-ms-date" {
			if len(headerValues) > 0 {
				xmsHeaders = append(xmsHeaders, header{
					name:  lowerName,
					value: headerValues[0],
				})
			}
		}
	}

	// Sort headers alphabetically by name
	sort.Slice(xmsHeaders, func(i, j int) bool {
		return xmsHeaders[i].name < xmsHeaders[j].name
	})

	// Build canonicalized headers string
	canonicalHeaders := ""
	for _, h := range xmsHeaders {
		canonicalHeaders += fmt.Sprintf("%s:%s\n", h.name, h.value)
	}

	// Canonicalized resource: /accountname/resourcepath
	canonicalResource := fmt.Sprintf("/%s%s", c.accountName, resourcePath)

	// Build StringToSign
	// Format: VERB\nContent-MD5\nContent-Type\nDate\nCanonicalizedHeaders\nCanonicalizedResource
	// When using x-ms-date, put it in the Date field
	stringToSign := fmt.Sprintf("%s\n\n%s\n%s\n%s%s",
		method,
		contentType,
		date,
		canonicalHeaders,
		canonicalResource,
	)

	// Compute HMAC-SHA256
	decodedKey, err := base64.StdEncoding.DecodeString(c.accountKey)
	if err != nil {
		// If key is not base64 encoded, use it as-is
		decodedKey = []byte(c.accountKey)
	}

	h := hmac.New(sha256.New, decodedKey)
	h.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	// Set Authorization header
	authHeader := fmt.Sprintf("SharedKey %s:%s", c.accountName, signature)
	req.Header.Set("Authorization", authHeader)
}
