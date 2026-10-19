package kaggleapi

import (
	"context"
	"crypto/sha256"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const MaxTransferBytes int64 = 4 << 30
const maxUploadBytes int64 = 2 << 30

// BlobUpload is an opaque single-use upload ticket. Its signed URL and blob token
// cannot be serialized or printed. It is bound to this client, name and size.
type BlobUpload struct {
	state *blobUploadState
}

type blobUploadState struct {
	mu     sync.Mutex
	client *Client
	url    string
	token  []byte
	name   string
	size   int64
	spent  bool
}

func (BlobUpload) String() string   { return "Kaggle blob upload ticket" }
func (BlobUpload) GoString() string { return "Kaggle blob upload ticket" }

// UploadedBlob is available only after a complete verified upload. Callers must
// still finish all local staging sources before issuing the dataset create.
type UploadedBlob struct {
	client *Client
	token  []byte
	name   string
	size   int64
}

func (UploadedBlob) String() string   { return "Kaggle uploaded blob" }
func (UploadedBlob) GoString() string { return "Kaggle uploaded blob" }

func (c *Client) StartBlobUpload(ctx context.Context, name string, size int64) (*BlobUpload, error) {
	if !safeFilePath(name) || strings.Contains(name, "/") || size < 0 || size > maxUploadBytes {
		return nil, failure(blobRPC, ErrConfig, 0, false)
	}
	query := struct {
		Type   string `json:"type"`
		Name   string `json:"name"`
		MIME   string `json:"contentType"`
		Length int64  `json:"contentLength,omitempty"`
	}{"DATASET", name, "application/octet-stream", size}
	object, err := c.rpc(ctx, blobRPC, query, maxAccountBytes)
	if err != nil {
		return nil, err
	}
	var receipt struct {
		Token string `json:"token"`
		URL   string `json:"createUrl"`
	}
	if decodeObject(object, &receipt) != nil || !validToken([]byte(receipt.Token)) || !signedURL(receipt.URL, true, false) {
		return nil, failure(blobRPC, ErrResponse, 0, true)
	}
	return &BlobUpload{state: &blobUploadState{client: c, url: receipt.URL, token: []byte(receipt.Token), name: name, size: size}}, nil
}

// UploadBlob takes ownership of source and closes it on every outcome. Source
// must unblock Read when Close is called so a deadline can stop local I/O too.
// Verification includes exact EOF and successful Close; no file token is returned
// after a short, excess, corrupt, interrupted or unacknowledged upload.
func (c *Client) UploadBlob(parent context.Context, ticket *BlobUpload, source io.ReadCloser, digest [sha256.Size]byte) (result *UploadedBlob, err error) {
	const op = "storage.PutBlob"
	if source == nil {
		return nil, failure(op, ErrConfig, 0, false)
	}
	owner := &sourceOwner{source: source}
	defer func() {
		if owner.Close() != nil && err == nil {
			result, err = nil, failure(op, ErrResponse, 0, true)
		}
	}()
	if c == nil || c.state == nil || ticket == nil || ticket.state == nil {
		return nil, failure(op, ErrConfig, 0, false)
	}
	ticket.state.mu.Lock()
	if ticket.state.client != c || ticket.state.spent {
		ticket.state.mu.Unlock()
		return nil, failure(op, ErrConfig, 0, false)
	}
	ticket.state.spent = true
	location, token, name, size := ticket.state.url, append([]byte(nil), ticket.state.token...), ticket.state.name, ticket.state.size
	ticket.state.url = ""
	clear(ticket.state.token)
	ticket.state.token = nil
	ticket.state.mu.Unlock()
	defer clear(token)
	ctx, cancel := context.WithTimeout(parent, c.state.transfer)
	defer cancel()
	stopClose := context.AfterFunc(ctx, func() { _ = owner.Close() })
	defer stopClose()
	body := &uploadBody{source: source, remaining: size, digest: sha256.New()}
	request, err := storageRequest(ctx, http.MethodPut, location, body, size, true, false)
	if err != nil {
		return nil, failure(op, ErrConfig, 0, false)
	}
	if size == 0 {
		request.Body = http.NoBody
	}
	response, err := roundTrip(ctx, c.state.storage, request, op)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, failure(op, ErrHTTP, response.StatusCode, true)
	}
	if _, err := readBounded(ctx, response, 64<<10); err != nil {
		return nil, failure(op, err, response.StatusCode, true)
	}
	if body.verify(digest) != nil || owner.Close() != nil {
		if ctx.Err() != nil {
			return nil, failure(op, ctx.Err(), response.StatusCode, true)
		}
		return nil, failure(op, ErrResponse, response.StatusCode, true)
	}
	if ctx.Err() != nil {
		return nil, failure(op, ctx.Err(), response.StatusCode, true)
	}
	return &UploadedBlob{client: c, token: append([]byte(nil), token...), name: name, size: size}, nil
}

type sourceOwner struct {
	source io.ReadCloser
	once   sync.Once
	err    error
}

func (s *sourceOwner) Close() error {
	s.once.Do(func() { s.err = s.source.Close() })
	return s.err
}

type uploadBody struct {
	mu        sync.Mutex
	source    io.Reader
	remaining int64
	digest    hash.Hash
	failed    bool
}

func (b *uploadBody) Read(buffer []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failed {
		return 0, ErrResponse
	}
	if len(buffer) == 0 {
		return 0, nil
	}
	if b.remaining == 0 {
		return 0, io.EOF
	}
	buffer = buffer[:min(int64(len(buffer)), b.remaining)]
	n, err := b.source.Read(buffer)
	if n < 0 || n > len(buffer) || err != nil && err != io.EOF || n == 0 || err == io.EOF && int64(n) < b.remaining {
		b.failed = true
		return 0, ErrResponse
	}
	b.remaining -= int64(n)
	_, _ = b.digest.Write(buffer[:n])
	return n, nil
}

// The HTTP transport owns this wrapper, while UploadBlob owns the actual source.
func (*uploadBody) Close() error { return nil }

func (b *uploadBody) verify(expected [sha256.Size]byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failed || b.remaining != 0 {
		return ErrResponse
	}
	var probe [1]byte
	n, err := b.source.Read(probe[:])
	if n != 0 || err != io.EOF || string(b.digest.Sum(nil)) != string(expected[:]) {
		return ErrResponse
	}
	return nil
}

type TransferReceipt struct {
	Bytes  int64
	SHA256 [sha256.Size]byte
}

func (c *Client) DownloadDatasetFile(ctx context.Context, resource Resource, name string, limit int64, destination io.Writer) (TransferReceipt, error) {
	op := datasetService + "DownloadDataset"
	if !resource.valid() || !safeFilePath(name) {
		return TransferReceipt{}, failure(op, ErrConfig, 0, false)
	}
	query := struct {
		Owner   string `json:"ownerSlug"`
		Slug    string `json:"datasetSlug"`
		Version int    `json:"datasetVersionNumber"`
		Name    string `json:"fileName"`
		Raw     bool   `json:"raw"`
	}{resource.Owner, resource.Slug, 1, name, true}
	return c.download(ctx, op, query, limit, destination, false)
}

func (c *Client) DownloadKernelOutput(ctx context.Context, resource Resource, path string, limit int64, destination io.Writer) (TransferReceipt, error) {
	op := kernelService + "DownloadKernelOutput"
	if !resource.valid() || !safeFilePath(path) {
		return TransferReceipt{}, failure(op, ErrConfig, 0, false)
	}
	query := struct {
		Owner   string `json:"ownerSlug"`
		Slug    string `json:"kernelSlug"`
		Version int    `json:"versionNumber"`
		Path    string `json:"filePath"`
	}{resource.Owner, resource.Slug, 1, path}
	return c.download(ctx, op, query, limit, destination, true)
}

func (c *Client) download(parent context.Context, op string, payload any, limit int64, destination io.Writer, kernel bool) (TransferReceipt, error) {
	if c == nil || c.state == nil || limit < 0 || limit > MaxTransferBytes || destination == nil {
		return TransferReceipt{}, failure(op, ErrConfig, 0, false)
	}
	ctx, cancel := context.WithTimeout(parent, c.state.transfer)
	defer cancel()
	result, err := c.requestResponse(ctx, op, readTries, func(ctx context.Context) (*rpcResponse, error) {
		return c.sendDownload(ctx, op, payload)
	})
	if err != nil {
		return TransferReceipt{}, err
	}
	defer result.cancel()
	response := result.response
	if response.StatusCode == 302 || response.StatusCode == 303 || response.StatusCode == 307 {
		locations := response.Header.Values("Location")
		_ = response.Body.Close() // Never consume an API redirect's body.
		if len(locations) != 1 {
			return TransferReceipt{}, failure(op, ErrResponse, response.StatusCode, true)
		}
		request, err := storageRequest(ctx, http.MethodGet, locations[0], nil, 0, false, kernel)
		if err != nil {
			return TransferReceipt{}, failure(op, ErrResponse, response.StatusCode, true)
		}
		response, err = roundTrip(ctx, c.state.storage, request, op)
		if err != nil {
			return TransferReceipt{}, err
		}
	} else if response.StatusCode == 200 && !contentType(response, "application/octet-stream") {
		_ = response.Body.Close()
		return TransferReceipt{}, failure(op, ErrResponse, response.StatusCode, true)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return TransferReceipt{}, failure(op, ErrHTTP, response.StatusCode, true)
	}
	receipt, err := copyBounded(ctx, response, limit, destination)
	if err == nil && response.Body.Close() != nil {
		err = ErrTransport
	}
	if err != nil {
		return TransferReceipt{}, failure(op, err, response.StatusCode, true)
	}
	return receipt, nil
}

func (c *Client) sendDownload(parent context.Context, op string, payload any) (*rpcResponse, error) {
	ctx, cancel := context.WithTimeout(parent, c.state.transfer)
	request, err := c.apiRequest(ctx, op, payload)
	if err != nil {
		cancel()
		return nil, err
	}
	request.Header.Set("Accept", "application/octet-stream")
	response, err := roundTrip(ctx, c.state.api, request, op)
	if err != nil {
		cancel()
		return nil, err
	}
	return &rpcResponse{response, ctx, cancel}, nil
}

func storageRequest(ctx context.Context, method, location string, body io.Reader, size int64, upload, kernel bool) (*http.Request, error) {
	if !signedURL(location, upload, kernel) {
		return nil, ErrConfig
	}
	request, err := http.NewRequestWithContext(ctx, method, location, body)
	if err != nil {
		return nil, ErrConfig
	}
	request.GetBody = nil
	request.Header.Set("Accept-Encoding", "identity")
	if upload {
		request.ContentLength = size
		request.Header.Set("Content-Type", "application/octet-stream")
	}
	return request, nil
}

func signedURL(location string, upload, kernel bool) bool {
	if len(location) < 1 || len(location) > 16384 || strings.Contains(location, "\\") {
		return false
	}
	for _, b := range []byte(location) {
		if b < 33 || b > 126 {
			return false
		}
	}
	parsed, err := url.Parse(location)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Fragment != "" || parsed.Opaque != "" || !strings.HasPrefix(parsed.Path, "/") {
		return false
	}
	switch parsed.Host {
	case "storage.googleapis.com", "storage.googleapis.com:443":
		return true
	case "www.googleapis.com", "www.googleapis.com:443":
		return upload && strings.HasPrefix(parsed.Path, "/upload/storage/v1/b/")
	case "www.kaggleusercontent.com", "www.kaggleusercontent.com:443":
		return !upload && kernel
	default:
		return false
	}
}

func copyBounded(ctx context.Context, response *http.Response, limit int64, destination io.Writer) (TransferReceipt, error) {
	declared, err := bodyLimit(response, limit)
	if err != nil {
		return TransferReceipt{}, err
	}
	digest := sha256.New()
	var count int64
	buffer := make([]byte, 64<<10)
	stalls := 0
	for {
		if ctx.Err() != nil {
			return TransferReceipt{}, ctx.Err()
		}
		n, readErr := response.Body.Read(buffer[:min(int64(len(buffer)), limit-count+1)])
		if n < 0 || n > len(buffer) || int64(n) > limit-count {
			return TransferReceipt{}, ErrLimit
		}
		if n > 0 {
			written, writeErr := destination.Write(buffer[:n])
			if writeErr != nil || written != n {
				return TransferReceipt{}, ErrTransport
			}
			_, _ = digest.Write(buffer[:n])
			count += int64(n)
			stalls = 0
		} else {
			stalls++
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil || stalls >= 10 {
			if ctx.Err() != nil {
				return TransferReceipt{}, ctx.Err()
			}
			return TransferReceipt{}, ErrTransport
		}
	}
	if ctx.Err() != nil {
		return TransferReceipt{}, ctx.Err()
	}
	if declared >= 0 && count != declared {
		return TransferReceipt{}, ErrResponse
	}
	var result TransferReceipt
	result.Bytes = count
	copy(result.SHA256[:], digest.Sum(nil))
	return result, nil
}
