package kaggleapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	apiBase          = "https://api.kaggle.com/v1/"
	authRPC          = "security.OAuthService/IntrospectToken"
	quotaRPC         = "kernels.KernelsApiService/GetAcceleratorQuotaStatistics"
	datasetService   = "datasets.DatasetApiService/"
	kernelService    = "kernels.KernelsApiService/"
	blobRPC          = "blobs.BlobApiService/StartBlobUpload"
	maxTokenBytes    = 8192
	maxRequestBytes  = 3 << 20
	maxMetadataBytes = 3 << 20
	maxAccountBytes  = 64 << 10
	maxErrorBytes    = 64 << 10
)

var (
	ErrConfig    = errors.New("invalid Kaggle API configuration or request")
	ErrTransport = errors.New("Kaggle API transport unavailable")
	ErrHTTP      = errors.New("Kaggle API HTTP failure")
	ErrResponse  = errors.New("invalid Kaggle API response")
	ErrLimit     = errors.New("Kaggle API size limit exceeded")
	ErrReceipt   = errors.New("Kaggle API completion unconfirmed")
	ErrQuota     = errors.New("free Kaggle GPU quota unverified")
)

// Error deliberately retains no provider body, URL, header or underlying error.
// Sent says the request reached the transport, not that its effect completed.
type Error struct {
	Operation      string
	StatusCode     int
	Sent           bool
	MissingName403 bool
	ProviderType   string // closed status enum only; never provider text
	kind           error
}

func (e *Error) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("%s: %s (HTTP %d)", e.Operation, e.kind, e.StatusCode)
	}
	return e.Operation + ": " + e.kind.Error()
}

func (e *Error) Unwrap() error { return e.kind }

func failure(op string, kind error, status int, sent bool) *Error {
	return &Error{Operation: op, StatusCode: status, Sent: sent, kind: kind}
}

// NameAbsent recognizes only exact Get failures used before creation. Cleanup
// must use received delete completion or an exact HTTP 404 as absence evidence.
func NameAbsent(err error) bool {
	var e *Error
	return errors.As(err, &e) && (e.Operation == datasetService+"GetDataset" || e.Operation == kernelService+"GetKernel") &&
		(e.StatusCode == http.StatusNotFound || e.MissingName403)
}

// Options' transports are trusted injection seams for offline fixtures. Production
// defaults use separate transports with TLS verification, no proxy, compression,
// keep-alive reuse or implicit transport retries. Timeouts can only reduce
// the fixed upper bounds.
type Options struct {
	APITransport     http.RoundTripper
	StorageTransport http.RoundTripper
	RequestTimeout   time.Duration
	TransferTimeout  time.Duration
	LogTimeout       time.Duration
	Pacing           *RequestPacer
	AccountScope     string
}

// Client owns a copy of the caller's credential. Close erases that copy. Supply
// credentials from a protected resolver or the caller's explicit environment;
// this package has no argument, environment or credential-file discovery path.
type Client struct {
	state *clientState
}

type clientState struct {
	mu            sync.RWMutex
	token         []byte
	api           http.RoundTripper
	storage       http.RoundTripper
	request       time.Duration
	createRequest time.Duration
	transfer      time.Duration
	logs          time.Duration
	pacing        *RequestPacer
	accountScope  string
	credentialKey [sha256.Size]byte
}

func (Client) String() string   { return "Kaggle API client" }
func (Client) GoString() string { return "Kaggle API client" }

func New(token []byte, options Options) (*Client, error) {
	if !validToken(token) || options.AccountScope != "" && !accountName.MatchString(options.AccountScope) {
		return nil, ErrConfig
	}
	request, ok := timeout(options.RequestTimeout, 15*time.Second, 30*time.Second)
	if !ok {
		return nil, ErrConfig
	}
	transfer, ok := timeout(options.TransferTimeout, 10*time.Minute, 30*time.Minute)
	if !ok {
		return nil, ErrConfig
	}
	logs, ok := timeout(options.LogTimeout, 15*time.Second, 15*time.Second)
	if !ok {
		return nil, ErrConfig
	}
	pacing := options.Pacing
	if pacing == nil {
		pacing = &RequestPacer{}
		if options.APITransport == nil {
			pacing = sharedRequestPacer
		}
	}
	if options.APITransport == nil {
		options.APITransport = transport(2 * time.Minute)
	}
	if options.StorageTransport == nil {
		options.StorageTransport = transport(10 * time.Second)
	}
	createRequest := options.RequestTimeout
	if createRequest == 0 {
		createRequest = 2 * time.Minute
	}
	return &Client{state: &clientState{token: bytes.Clone(token), api: options.APITransport, storage: options.StorageTransport,
		request: request, createRequest: createRequest, transfer: transfer, logs: logs, pacing: pacing, accountScope: options.AccountScope, credentialKey: sha256.Sum256(token)}}, nil
}

func timeout(value, fallback, maximum time.Duration) (time.Duration, bool) {
	if value == 0 {
		return fallback, true
	}
	return value, value > 0 && value <= maximum
}

func validToken(token []byte) bool {
	if len(token) == 0 || len(token) > maxTokenBytes {
		return false
	}
	for _, b := range token {
		if b < 33 || b > 126 {
			return false
		}
	}
	return true
}

func transport(headerTimeout time.Duration) *http.Transport {
	return &http.Transport{
		Proxy:                  nil,
		DialContext:            (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		TLSHandshakeTimeout:    5 * time.Second,
		ResponseHeaderTimeout:  headerTimeout,
		DisableCompression:     true,
		DisableKeepAlives:      true,
		MaxResponseHeaderBytes: 64 << 10,
	}
}

func (c *Client) Close() error {
	if c == nil || c.state == nil {
		return nil
	}
	c.state.mu.Lock()
	clear(c.state.token)
	c.state.token = nil
	c.state.mu.Unlock()
	for _, rt := range []http.RoundTripper{c.state.api, c.state.storage} {
		if closer, ok := rt.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
	}
	return nil
}

func (c *Client) credential() []byte {
	if c == nil || c.state == nil {
		return nil
	}
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	return bytes.Clone(c.state.token)
}

func (c *Client) requestScope() string {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	if c.state.accountScope != "" {
		return c.state.accountScope
	}
	return "token:" + hex.EncodeToString(c.state.credentialKey[:])
}

func (c *Client) apiRequest(ctx context.Context, op string, payload any) (*http.Request, error) {
	token := c.credential()
	defer clear(token)
	if len(token) == 0 {
		return nil, failure(op, ErrConfig, 0, false)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, failure(op, ErrConfig, 0, false)
	}
	if len(data) > maxRequestBytes {
		return nil, failure(op, ErrLimit, 0, false)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, apiBase+op, bytes.NewReader(data))
	if err != nil {
		return nil, failure(op, ErrConfig, 0, false)
	}
	request.GetBody = nil // A replayable body must never make a mutation retryable.
	request.Header.Set("Authorization", "Bearer "+string(token))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	return request, nil
}

// RoundTrip is used directly, so there is no redirect-following Client or Jar.
func roundTrip(ctx context.Context, rt http.RoundTripper, request *http.Request, op string) (*http.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, failure(op, err, 0, false)
	}
	response, err := rt.RoundTrip(request)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		kind := ErrTransport
		if ctx.Err() != nil {
			kind = ctx.Err()
		} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			kind = context.DeadlineExceeded
		}
		return nil, failure(op, kind, 0, true)
	}
	if response == nil || response.Body == nil || response.StatusCode < 100 || response.StatusCode > 599 {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, failure(op, ErrResponse, 0, true)
	}
	return response, nil
}

func (c *Client) rpc(parent context.Context, op string, payload any, limit int64) (map[string]json.RawMessage, error) {
	if c == nil || c.state == nil {
		return nil, failure(op, ErrConfig, 0, false)
	}
	response, err := c.rpcResponse(parent, op, payload)
	if err != nil {
		return nil, err
	}
	defer response.cancel()
	return decodeRPC(response.ctx, op, response.response, limit)
}

func (c *Client) sendRPC(parent context.Context, op string, payload any) (*rpcResponse, error) {
	budget := c.state.request
	// Dataset creation can persist uploaded blobs before answering. Give its one
	// send a bounded acknowledgement budget; reads retain their short deadlines.
	if op == datasetService+"CreateDataset" {
		budget = c.state.createRequest
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	request, err := c.apiRequest(ctx, op, payload)
	if err != nil {
		cancel()
		return nil, err
	}
	response, err := roundTrip(ctx, c.state.api, request, op)
	if err != nil {
		cancel()
		return nil, err
	}
	return &rpcResponse{response, ctx, cancel}, nil
}

func decodeRPC(ctx context.Context, op string, response *http.Response, limit int64) (map[string]json.RawMessage, error) {
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, httpFailure(ctx, op, response)
	}
	if (op == datasetService+"DeleteDataset" || op == kernelService+"DeleteKernel") && response.StatusCode != 200 {
		return nil, failure(op, ErrReceipt, response.StatusCode, true)
	}
	if !contentType(response, "application/json") {
		return nil, failure(op, ErrResponse, response.StatusCode, true)
	}
	data, err := readBounded(ctx, response, limit)
	if err != nil {
		return nil, failure(op, err, response.StatusCode, true)
	}
	object, err := strictObject(data)
	if err != nil {
		return nil, failure(op, ErrResponse, response.StatusCode, true)
	}
	if raw, ok := object["code"]; ok {
		var code int
		if json.Unmarshal(raw, &code) != nil {
			return nil, failure(op, ErrResponse, response.StatusCode, true)
		}
		if code >= 400 {
			return nil, failure(op, ErrHTTP, response.StatusCode, true)
		}
	}
	return object, nil
}

func httpFailure(ctx context.Context, op string, response *http.Response) error {
	err := failure(op, ErrHTTP, response.StatusCode, true)
	permission := ""
	if op == datasetService+"GetDataset" {
		permission = "datasets.get"
	} else if op == kernelService+"GetKernel" {
		permission = "kernels.get"
	}
	// Only exact-name reads and submission errors need body classification.
	// Retain a closed provider enum and an absence boolean, never body text.
	if (permission != "" && response.StatusCode == 403 || op == kernelService+"SaveKernel") && contentType(response, "application/json") {
		data, readErr := readBounded(ctx, response, maxErrorBytes)
		if readErr == nil {
			object, parseErr := strictObject(data)
			var envelope struct {
				Code    int    `json:"code"`
				Status  string `json:"status"`
				Message string `json:"message"`
			}
			errorObject, errorParseErr := strictObject(object["error"])
			if parseErr == nil && errorParseErr == nil && decodeObject(errorObject, &envelope) == nil {
				switch envelope.Status {
				case "PERMISSION_DENIED", "UNAUTHENTICATED", "RESOURCE_EXHAUSTED", "UNAVAILABLE", "DEADLINE_EXCEEDED", "INVALID_ARGUMENT", "INTERNAL", "NOT_FOUND", "ALREADY_EXISTS":
					err.ProviderType = envelope.Status
				}
				err.MissingName403 = permission != "" && envelope.Code == 403 && envelope.Status == "PERMISSION_DENIED" && envelope.Message == "Permission '"+permission+"' was denied"
			}
		}
	}
	return err
}

func contentType(response *http.Response, expected string) bool {
	values := response.Header.Values("Content-Type")
	if len(values) != 1 {
		return false
	}
	media, _, err := mime.ParseMediaType(values[0])
	return err == nil && media == expected
}

func bodyLimit(response *http.Response, limit int64) (int64, error) {
	if encoding := strings.TrimSpace(response.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return 0, ErrResponse
	}
	declared := int64(-1)
	values := response.Header.Values("Content-Length")
	if len(values) > 1 {
		return 0, ErrResponse
	}
	if len(values) == 1 {
		var ok bool
		declared, ok = unsignedDecimal(values[0], limit)
		if !ok {
			return 0, ErrLimit
		}
	} else if response.ContentLength >= 0 {
		declared = response.ContentLength
	}
	if declared > limit || declared < -1 {
		return 0, ErrLimit
	}
	return declared, nil
}

func readBounded(ctx context.Context, response *http.Response, limit int64) ([]byte, error) {
	declared, err := bodyLimit(response, limit)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, ErrTransport
	}
	if int64(len(data)) > limit {
		return nil, ErrLimit
	}
	if declared >= 0 && int64(len(data)) != declared {
		return nil, ErrResponse
	}
	return data, nil
}
