// Package client is a Go client for a raftkv cluster. It follows leader
// redirects, rotates through endpoints on failure, and retries writes with the
// same session sequence number so the cluster applies each write exactly once
// even when a reply is lost.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Header names of the session identity; they match the server's.
const (
	headerClientID = "Raftkv-Client-Id"
	headerSeq      = "Raftkv-Seq"
)

// Result is the outcome of a write.
type Result struct {
	// Applied is false for a CAS whose precondition failed, or a delete of a
	// missing key.
	Applied bool `json:"applied"`
	// PrevFound and Prev describe the value before the operation.
	PrevFound bool   `json:"prev_found"`
	Prev      string `json:"prev"`
	// Duplicate is true when the server answered a retry from its session table.
	Duplicate bool `json:"duplicate"`
}

// APIError is a definitive error response from the cluster.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("raftkv: %s (HTTP %d): %s", e.Code, e.Status, e.Message)
}

// Client talks to a cluster. Writes are serialised because a session may have
// only one command outstanding.
type Client struct {
	http        *http.Client
	maxAttempts int
	backoff     time.Duration

	clientID uint64
	writeMu  sync.Mutex
	seq      uint64

	mu        sync.Mutex
	endpoints []string
	current   int
}

// Option customises a Client.
type Option func(*Client)

// WithHTTPClient replaces the HTTP client. Its redirect policy is overridden.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithMaxAttempts bounds how many requests one operation may issue.
func WithMaxAttempts(n int) Option { return func(c *Client) { c.maxAttempts = n } }

// WithBackoff sets the base delay between failed attempts.
func WithBackoff(d time.Duration) Option { return func(c *Client) { c.backoff = d } }

// WithClientID pins the session id, for callers that persist it.
func WithClientID(id uint64) Option { return func(c *Client) { c.clientID = id } }

// New creates a client for the given base URLs (any subset of the cluster).
func New(endpoints []string, opts ...Option) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("client: at least one endpoint is required")
	}
	c := &Client{maxAttempts: 30, backoff: 50 * time.Millisecond, clientID: rand.Uint64N(1<<62) + 1}
	for _, e := range endpoints {
		u, err := url.Parse(e)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("client: invalid endpoint %q", e)
		}
		c.endpoints = append(c.endpoints, strings.TrimRight(e, "/"))
	}
	for _, o := range opts {
		o(c)
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 10 * time.Second}
	}
	hc := *c.http
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.http = &hc
	return c, nil
}

// ClientID returns the session id used for writes.
func (c *Client) ClientID() uint64 { return c.clientID }

// Get reads a key linearizably. found is false if the key does not exist.
func (c *Client) Get(ctx context.Context, key string) (value string, found bool, err error) {
	status, body, err := c.do(ctx, http.MethodGet, "/v1/kv/"+escapeKey(key), nil, 0)
	if err != nil {
		return "", false, err
	}
	if status == http.StatusNotFound {
		return "", false, nil
	}
	if status != http.StatusOK {
		return "", false, apiError(status, body)
	}
	var out struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", false, fmt.Errorf("client: decode response: %w", err)
	}
	return out.Value, true, nil
}

// Put writes a value.
func (c *Client) Put(ctx context.Context, key, value string) (Result, error) {
	return c.write(ctx, http.MethodPut, "/v1/kv/"+escapeKey(key), map[string]string{"value": value})
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key string) (Result, error) {
	return c.write(ctx, http.MethodDelete, "/v1/kv/"+escapeKey(key), nil)
}

// CompareAndSwap sets key to value only if its current value equals expected.
func (c *Client) CompareAndSwap(ctx context.Context, key, expected, value string) (Result, error) {
	return c.write(ctx, http.MethodPost, "/v1/cas/"+escapeKey(key),
		map[string]any{"expected": expected, "value": value})
}

// CreateIfAbsent sets key to value only if the key does not exist.
func (c *Client) CreateIfAbsent(ctx context.Context, key, value string) (Result, error) {
	return c.write(ctx, http.MethodPost, "/v1/cas/"+escapeKey(key),
		map[string]any{"expect_absent": true, "value": value})
}

// NodeStatus is the /v1/status document of one node.
type NodeStatus struct {
	ID            uint64 `json:"id"`
	Role          string `json:"role"`
	Term          uint64 `json:"term"`
	Leader        uint64 `json:"leader"`
	CommitIndex   uint64 `json:"commit_index"`
	AppliedIndex  uint64 `json:"applied_index"`
	LastIndex     uint64 `json:"last_index"`
	SnapshotIndex uint64 `json:"snapshot_index"`
	Keys          int    `json:"keys"`
}

// Status asks one specific endpoint for its status, without redirects or retries.
func (c *Client) Status(ctx context.Context, endpoint string) (NodeStatus, error) {
	var st NodeStatus
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/v1/status", nil)
	if err != nil {
		return st, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return st, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("client: status from %s: %s", endpoint, resp.Status)
	}
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

func (c *Client) write(ctx context.Context, method, path string, body any) (Result, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.seq++
	status, raw, err := c.do(ctx, method, path, body, c.seq)
	if err != nil {
		return Result{}, err
	}
	if status != http.StatusOK {
		return Result{}, apiError(status, raw)
	}
	var res Result
	if err := json.Unmarshal(raw, &res); err != nil {
		return Result{}, fmt.Errorf("client: decode response: %w", err)
	}
	return res, nil
}

func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func apiError(status int, body []byte) error {
	e := &APIError{Status: status, Code: "unknown", Message: strings.TrimSpace(string(body))}
	var parsed struct{ Error, Message string }
	if json.Unmarshal(body, &parsed) == nil && parsed.Error != "" {
		e.Code, e.Message = parsed.Error, parsed.Message
	}
	return e
}

func (c *Client) base() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.endpoints[c.current]
}

func (c *Client) rotate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = (c.current + 1) % len(c.endpoints)
}

// follow makes the redirect target the current endpoint, remembering it.
func (c *Client) follow(location string) bool {
	u, err := url.Parse(location)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	base := u.Scheme + "://" + u.Host
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, e := range c.endpoints {
		if e == base {
			c.current = i
			return true
		}
	}
	c.endpoints = append(c.endpoints, base)
	c.current = len(c.endpoints) - 1
	return true
}

func (c *Client) sleep(ctx context.Context, attempt int) error {
	d := min(c.backoff*time.Duration(attempt), 10*c.backoff)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// do issues the request until it gets a definitive answer. Retried writes carry
// the same seq, which is what makes retrying safe.
func (c *Client) do(ctx context.Context, method, path string, body any, seq uint64) (int, []byte, error) {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return 0, nil, err
		}
	}
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.base()+path, bytes.NewReader(payload))
		if err != nil {
			return 0, nil, err
		}
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if seq != 0 {
			req.Header.Set(headerClientID, strconv.FormatUint(c.clientID, 10))
			req.Header.Set(headerSeq, strconv.FormatUint(seq, 10))
		}
		resp, err := c.http.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return 0, nil, ctx.Err()
			}
			lastErr = err
			c.rotate()
			if err := c.sleep(ctx, attempt); err != nil {
				return 0, nil, err
			}
			continue
		}
		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			c.rotate()
			continue
		}
		switch resp.StatusCode {
		case http.StatusTemporaryRedirect:
			if c.follow(resp.Header.Get("Location")) {
				continue
			}
			lastErr = errors.New("client: malformed redirect")
		case http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			lastErr = apiError(resp.StatusCode, raw)
			var ae *APIError
			if errors.As(lastErr, &ae) && ae.Code == "no_leader" {
				c.rotate()
			}
		default:
			return resp.StatusCode, raw, nil
		}
		if err := c.sleep(ctx, attempt); err != nil {
			return 0, nil, err
		}
	}
	return 0, nil, fmt.Errorf("client: gave up after %d attempts: %w", c.maxAttempts, lastErr)
}
