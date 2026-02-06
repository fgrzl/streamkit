package azurekit

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// HTTP client configuration
	HTTPRequestTimeout      = 30 * time.Second
	HTTPConnectTimeout      = 5 * time.Second
	HTTPKeepAlive           = 30 * time.Second
	HTTPIdleConnTimeout     = 90 * time.Second
	HTTPTLSHandshakeTimeout = 10 * time.Second
	HTTPMaxIdleConns        = 100
	HTTPMaxIdleConnsPerHost = 100

	// Azure Table Storage limits
	AzureBatchMaxEntities = 100
	AzureDefaultPageSize  = 1000
	AzureAPIVersion       = "2021-06-08"

	// Retry configuration
	MaxRetryAttempts  = 3
	InitialRetryDelay = 100 * time.Millisecond
	MaxRetryDelay     = 10 * time.Second

	// Buffer pool size hint
	BufferPoolDefaultSize = 32 * 1024 // 32KB
)

// AzureError represents a structured error from Azure Table Storage
type AzureError struct {
	StatusCode int
	RequestID  string
	Message    string
	Code       string
}

func (e *AzureError) Error() string {
	return fmt.Sprintf("azure table error: status=%d code=%s request_id=%s message=%s",
		e.StatusCode, e.Code, e.RequestID, e.Message)
}

// IsTransient returns true if the error is likely transient and retryable
func (e *AzureError) IsTransient() bool {
	return e.StatusCode == http.StatusTooManyRequests ||
		e.StatusCode == http.StatusServiceUnavailable ||
		e.StatusCode == http.StatusRequestTimeout
}

// ErrorResponse represents Azure error response body
type ErrorResponse struct {
	ODataError struct {
		Code    string `json:"code"`
		Message struct {
			Lang  string `json:"lang"`
			Value string `json:"value"`
		} `json:"message"`
	} `json:"odata.error"`
}

// parseAzureError extracts structured error information from Azure response
func parseAzureError(resp *http.Response, body []byte) *AzureError {
	azErr := &AzureError{
		StatusCode: resp.StatusCode,
		RequestID:  resp.Header.Get("x-ms-request-id"),
		Message:    string(body),
	}

	// Try to parse structured error response
	var errResp ErrorResponse
	if err := json.Unmarshal(body, &errResp); err == nil {
		if errResp.ODataError.Code != "" {
			azErr.Code = errResp.ODataError.Code
			azErr.Message = errResp.ODataError.Message.Value
		}
	}

	return azErr
}

// shouldRetry determines if a request should be retried based on error type
func shouldRetry(err error) bool {
	var azErr *AzureError
	if errors.As(err, &azErr) {
		return azErr.IsTransient()
	}
	// Also retry on network errors
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// getRetryDelay calculates exponential backoff delay with optional Retry-After header
func getRetryDelay(attempt int, resp *http.Response) time.Duration {
	// Check Retry-After header for 429 responses
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if seconds, err := strconv.Atoi(retryAfter); err == nil {
				return time.Duration(seconds) * time.Second
			}
		}
	}

	// Exponential backoff
	delay := InitialRetryDelay * (1 << uint(attempt))
	if delay > MaxRetryDelay {
		delay = MaxRetryDelay
	}
	return delay
}

// retryableRequest executes a request with automatic retry on transient failures
// Returns the response and any non-retryable error
func (c *HTTPTableClient) retryableRequest(ctx context.Context, req *http.Request, body []byte) (*http.Response, error) {
	var lastErr error
	var resp *http.Response

	for attempt := 0; attempt < MaxRetryAttempts; attempt++ {
		// Check context cancellation before each attempt
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context cancelled: %w", err)
		}

		// Clone request for retry (body needs to be re-readable)
		var reqBody io.Reader
		if len(body) > 0 {
			reqBody = bytes.NewReader(body)
		}
		retryReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL.String(), reqBody)
		if err != nil {
			return nil, fmt.Errorf("create retry request: %w", err)
		}

		// Copy headers
		retryReq.Header = req.Header.Clone()

		resp, err = c.httpClient.Do(retryReq)
		if err != nil {
			// Network error - check if retryable
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() && attempt < MaxRetryAttempts-1 {
				delay := getRetryDelay(attempt, nil)
				slog.Warn("request timeout, retrying",
					"attempt", attempt+1,
					"delay", delay,
					"error", err)
				time.Sleep(delay)
				lastErr = err
				continue
			}
			return nil, fmt.Errorf("request failed: %w", err)
		}

		// Check for transient HTTP errors
		if resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusServiceUnavailable ||
			resp.StatusCode == http.StatusRequestTimeout {

			if attempt < MaxRetryAttempts-1 {
				delay := getRetryDelay(attempt, resp)
				respBody, _ := io.ReadAll(resp.Body)
				resp.Body.Close()

				slog.Warn("transient error, retrying",
					"status", resp.StatusCode,
					"attempt", attempt+1,
					"delay", delay,
					"request_id", resp.Header.Get("x-ms-request-id"))

				time.Sleep(delay)
				lastErr = parseAzureError(resp, respBody)
				continue
			} else {
				// Final attempt failed
				respBody, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				return nil, parseAzureError(resp, respBody)
			}
		}

		// Success or non-retryable error
		return resp, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
	}
	return nil, errors.New("max retries exceeded")
}

var (
	// bufferPool for reusing byte buffers with pre-allocated capacity
	bufferPool = sync.Pool{
		New: func() interface{} {
			buf := bytes.NewBuffer(make([]byte, 0, BufferPoolDefaultSize))
			return buf
		},
	}

	// builderPool for reusing strings.Builder with pre-allocated capacity
	builderPool = sync.Pool{
		New: func() interface{} {
			b := &strings.Builder{}
			b.Grow(512) // Pre-allocate reasonable URL size
			return b
		},
	}

	// hmacKeyPool for reusing HMAC hashers (pre-compute key, clone per-request)
	hmacKeyPool = sync.Pool{
		New: func() interface{} {
			return nil // Will be set per-client
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
// Disables compression since stored data is already compressed (zstd events)
func newOptimizedHTTPClient() *http.Client {
	return &http.Client{
		Timeout: HTTPRequestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        HTTPMaxIdleConns,
			MaxIdleConnsPerHost: HTTPMaxIdleConnsPerHost,
			IdleConnTimeout:     HTTPIdleConnTimeout,
			DisableCompression:  true, // Data is already compressed
			DialContext: (&net.Dialer{
				Timeout:   HTTPConnectTimeout,
				KeepAlive: HTTPKeepAlive,
			}).DialContext,
			TLSHandshakeTimeout:   HTTPTLSHandshakeTimeout,
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
		return fmt.Errorf("create request: %w", err)
	}

	c.signRequest(req, "POST", data, "/Tables")
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// Table already exists is acceptable (409)
	if resp.StatusCode == http.StatusConflict {
		slog.Debug("table already exists", "table", c.tableName)
		return nil
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return parseAzureError(resp, body)
	}

	slog.Info("table created", "table", c.tableName, "request_id", resp.Header.Get("x-ms-request-id"))
	return nil
}

// AddEntity inserts a new entity (insert-only semantics)
func (c *HTTPTableClient) AddEntity(ctx context.Context, data []byte) error {
	var e entity
	if err := json.Unmarshal(data, &e); err != nil {
		return fmt.Errorf("unmarshal entity: %w", err)
	}

	reqBody := map[string]interface{}{
		"PartitionKey": e.PartitionKey,
		"RowKey":       e.RowKey,
	}
	if len(e.Value) > 0 {
		reqBody["Value"] = base64.StdEncoding.EncodeToString(e.Value)
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	enc := json.NewEncoder(buf)
	if err := enc.Encode(reqBody); err != nil {
		return fmt.Errorf("encode entity: %w", err)
	}

	// Use odata=nometadata for minimal response
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	// Pre-calculate and grow to exact size
	expectedLen := len(c.endpoint) + 1 + len(c.tableName)
	if c.useSAS {
		expectedLen += 1 + len(c.sasToken)
	}
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteByte('/')
	b.WriteString(c.tableName)
	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	c.signRequest(req, "POST", buf.Bytes(), fmt.Sprintf("/%s", c.tableName))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return parseAzureError(resp, body)
	}

	return nil
}

// AddEntityBatch inserts multiple entities in a single batch request (up to 100)
// Uses multipart/mixed format for efficient bulk inserts
func (c *HTTPTableClient) AddEntityBatch(ctx context.Context, entities [][]byte) error {
	if len(entities) == 0 {
		return nil
	}
	if len(entities) > AzureBatchMaxEntities {
		return fmt.Errorf("batch size %d exceeds maximum of %d entities", len(entities), AzureBatchMaxEntities)
	}

	// Generate batch and changeset boundaries
	batchID := fmt.Sprintf("batch_%d", time.Now().UnixNano())
	changesetID := fmt.Sprintf("changeset_%d", time.Now().UnixNano())

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Pre-allocate buffer with reasonable estimate (reduce allocations)
	// Rough estimate: 500 bytes per entity + batch overhead
	estimatedSize := len(entities)*500 + 1024
	if buf.Cap() < estimatedSize {
		buf.Grow(estimatedSize - buf.Len())
	}

	// Build multipart/mixed request
	fmt.Fprintf(buf, "--%s\r\n", batchID)
	fmt.Fprintf(buf, "Content-Type: multipart/mixed; boundary=%s\r\n\r\n", changesetID)

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

		fmt.Fprintf(buf, "--%s\r\n", changesetID)
		fmt.Fprintf(buf, "Content-Type: application/http\r\n")
		fmt.Fprintf(buf, "Content-Transfer-Encoding: binary\r\n\r\n")
		fmt.Fprintf(buf, "POST %s/%s HTTP/1.1\r\n", c.endpoint, c.tableName)
		fmt.Fprintf(buf, "Content-Type: application/json\r\n")
		fmt.Fprintf(buf, "Accept: application/json;odata=nometadata\r\n")
		fmt.Fprintf(buf, "Content-Length: %d\r\n\r\n", len(entJSON))
		buf.Write(entJSON)
		buf.WriteString("\r\n")
	}

	fmt.Fprintf(buf, "--%s--\r\n", changesetID)
	fmt.Fprintf(buf, "--%s--\r\n", batchID)

	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	expectedLen := len(c.endpoint) + 7 // "/$batch"
	if c.useSAS {
		expectedLen += 1 + len(c.sasToken)
	}
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteString("/$batch")
	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	c.signRequest(req, "POST", buf.Bytes(), "/$batch")
	req.Header.Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", batchID))
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute batch request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return parseAzureError(resp, body)
	}

	slog.Debug("batch entities added",
		"count", len(entities),
		"request_id", resp.Header.Get("x-ms-request-id"))

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

	// Use buffer pool for JSON encoding
	bodyBuf := bufferPool.Get().(*bytes.Buffer)
	bodyBuf.Reset()
	defer bufferPool.Put(bodyBuf)

	if err := json.NewEncoder(bodyBuf).Encode(reqBody); err != nil {
		return err
	}
	body := bodyBuf.Bytes()

	// Build URL efficiently
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	// Pre-calculate URL length for efficiency
	expectedLen := len(c.endpoint) + 1 + len(c.tableName) + 16 + // "(PartitionKey=''"
		len(e.PartitionKey)*3 + 11 + len(e.RowKey)*3 + 2 // URL escape worst case is 3x
	if c.useSAS {
		expectedLen += 1 + len(c.sasToken)
	}
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteByte('/')
	b.WriteString(c.tableName)
	b.WriteString("(PartitionKey='")
	b.WriteString(url.QueryEscape(e.PartitionKey))
	b.WriteString("',RowKey='")
	b.WriteString(url.QueryEscape(e.RowKey))
	b.WriteString("')")

	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	reqURL := b.String()

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
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return parseAzureError(resp, body)
	}

	return nil
}

// DeleteEntity removes an entity
func (c *HTTPTableClient) DeleteEntity(ctx context.Context, pk, rk string) error {
	// Build URL efficiently
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	b.WriteString(c.endpoint)
	b.WriteByte('/')
	b.WriteString(c.tableName)
	b.WriteString("(PartitionKey='")
	b.WriteString(url.QueryEscape(pk))
	b.WriteString("',RowKey='")
	b.WriteString(url.QueryEscape(rk))
	b.WriteString("')")

	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	}
	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "DELETE", reqURL, nil)
	if err != nil {
		return err
	}

	c.signRequest(req, "DELETE", nil, fmt.Sprintf("/%s(PartitionKey='%s',RowKey='%s')",
		c.tableName, url.QueryEscape(pk), url.QueryEscape(rk)))
	req.Header.Set("If-Match", "*") // Delete regardless of etag

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// 404 is acceptable (entity doesn't exist)
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return parseAzureError(resp, body)
	}

	return nil
}

// GetEntity retrieves a single entity
func (c *HTTPTableClient) GetEntity(ctx context.Context, pk, rk string) ([]byte, error) {
	// Build URL efficiently
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	expectedLen := len(c.endpoint) + 1 + len(c.tableName) + 16 +
		len(pk)*3 + 11 + len(rk)*3 + 2 + 13 // "?$format=json"
	if c.useSAS {
		expectedLen += 1 + len(c.sasToken)
	}
	b.Grow(expectedLen)

	b.WriteString(c.endpoint)
	b.WriteByte('/')
	b.WriteString(c.tableName)
	b.WriteString("(PartitionKey='")
	b.WriteString(url.QueryEscape(pk))
	b.WriteString("',RowKey='")
	b.WriteString(url.QueryEscape(rk))
	b.WriteString("')")

	if c.useSAS {
		b.WriteByte('?')
		b.WriteString(c.sasToken)
	} else {
		b.WriteString("?$format=json")
	}
	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}

	c.signRequest(req, "GET", nil, fmt.Sprintf("/%s(PartitionKey='%s',RowKey='%s')",
		c.tableName, url.QueryEscape(pk), url.QueryEscape(rk)))
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("ResourceNotFound")
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, parseAzureError(resp, body)
	}

	var result entity
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
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
	if top == 0 || top > AzureDefaultPageSize {
		top = AzureDefaultPageSize
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

// FetchPage fetches the next page of results with efficient continuation token handling
func (p *ListEntitiesPager) FetchPage(ctx context.Context) ([]entity, error) {
	if p.done {
		return nil, nil
	}

	p.ctx = ctx

	// Build query URL efficiently with strings.Builder
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	// Pre-allocate with reasonable estimate
	estimatedLen := len(p.client.endpoint) + 1 + len(p.client.tableName) + 256
	if p.client.useSAS {
		estimatedLen += len(p.client.sasToken)
	}
	b.Grow(estimatedLen)

	b.WriteString(p.client.endpoint)
	b.WriteByte('/')
	b.WriteString(p.client.tableName)
	b.WriteByte('?')
	b.WriteString("$top=")
	b.WriteString(strconv.Itoa(int(p.top)))

	if p.filter != "" {
		b.WriteString("&$filter=")
		b.WriteString(url.QueryEscape(p.filter))
	}
	if p.selectCols != "" {
		b.WriteString("&$select=")
		b.WriteString(url.QueryEscape(p.selectCols))
	}
	if p.continuationToken != "" {
		b.WriteByte('&')
		b.WriteString(p.continuationToken)
	}

	// Add SAS token if configured
	if p.client.useSAS {
		b.WriteByte('&')
		b.WriteString(p.client.sasToken)
	}

	reqURL := b.String()

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}

	p.client.signRequest(req, "GET", nil, fmt.Sprintf("/%s", p.client.tableName))
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := p.client.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, parseAzureError(resp, body)
	}

	// Extract continuation tokens from response headers
	nextPK := resp.Header.Get("x-ms-continuation-NextPartitionKey")
	nextRK := resp.Header.Get("x-ms-continuation-NextRowKey")

	if nextPK != "" || nextRK != "" {
		// Build continuation token query string
		cb := builderPool.Get().(*strings.Builder)
		cb.Reset()
		if nextPK != "" {
			cb.WriteString("NextPartitionKey=")
			cb.WriteString(url.QueryEscape(nextPK))
		}
		if nextRK != "" {
			if cb.Len() > 0 {
				cb.WriteByte('&')
			}
			cb.WriteString("NextRowKey=")
			cb.WriteString(url.QueryEscape(nextRK))
		}
		p.continuationToken = cb.String()
		builderPool.Put(cb)
	} else {
		p.done = true
	}

	var pageResp PageResponse
	if err := json.NewDecoder(resp.Body).Decode(&pageResp); err != nil {
		return nil, err
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
	req.Header.Set("x-ms-version", AzureAPIVersion)
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
			// Log error but don't fail - request will likely get 401
			slog.Error("failed to acquire managed identity token",
				"error", err,
				"method", method,
				"path", resourcePath)
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
