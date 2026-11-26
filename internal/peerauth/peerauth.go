// Package peerauth authenticates node-to-node Raft traffic with a shared
// cluster key. Every message carries an HMAC-SHA256 over its sender, receiver,
// term, type, a hash of the body, a timestamp and a random nonce. The receiver
// verifies the MAC in constant time, rejects timestamps outside a skew window,
// and remembers recent nonces in a bounded cache so a captured message cannot
// be replayed.
//
// The key proves "this message came from someone holding the cluster key"; it
// does not distinguish one node from another, so a compromised node can still
// impersonate its peers. Per-node keys or mTLS would be the next step.
package peerauth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/FrancisWorld/raftkv/internal/raft"
)

// Header names carrying the authentication data.
const (
	HeaderTimestamp = "Raftkv-Peer-Timestamp"
	HeaderNonce     = "Raftkv-Peer-Nonce"
	HeaderMAC       = "Raftkv-Peer-Mac"
)

// MinKeyLen is the minimum accepted key length in bytes.
const MinKeyLen = 32

// Verification failures. Callers can use them as metric labels.
var (
	ErrMissing   = errors.New("peerauth: missing authentication headers")
	ErrBadMAC    = errors.New("peerauth: bad MAC")
	ErrStale     = errors.New("peerauth: timestamp outside the allowed skew")
	ErrReplay    = errors.New("peerauth: replayed message")
	ErrCacheFull = errors.New("peerauth: replay cache full")
)

// Options tune an Authenticator.
type Options struct {
	// Skew is the accepted difference between sender and receiver clocks.
	Skew time.Duration
	// MaxNonces bounds the replay cache. When it is full, messages are rejected
	// rather than risking a replay slipping through.
	MaxNonces int
	// Now replaces time.Now in tests.
	Now func() time.Time
}

type seenNonce struct {
	nonce   string
	expires time.Time
}

// Authenticator signs outgoing and verifies incoming peer messages.
type Authenticator struct {
	key  []byte
	skew time.Duration
	max  int
	now  func() time.Time

	mu    sync.Mutex
	seen  map[string]struct{}
	order []seenNonce // insertion order; expiry is monotonic
}

// New returns an Authenticator for key, which must be at least MinKeyLen bytes.
func New(key []byte, opts Options) (*Authenticator, error) {
	if len(key) < MinKeyLen {
		return nil, fmt.Errorf("peerauth: cluster key must be at least %d bytes", MinKeyLen)
	}
	if opts.Skew == 0 {
		opts.Skew = 30 * time.Second
	}
	if opts.MaxNonces == 0 {
		opts.MaxNonces = 1 << 18
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Authenticator{
		key: append([]byte(nil), key...), skew: opts.Skew, max: opts.MaxNonces, now: opts.Now,
		seen: map[string]struct{}{},
	}, nil
}

// LoadKey reads a cluster key from a file (preferred) or, if path is empty,
// from the environment variable envName. Surrounding whitespace is trimmed.
func LoadKey(path, envName string) ([]byte, error) {
	var raw string
	switch {
	case path != "":
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read cluster key file: %w", err)
		}
		raw = string(b)
	case os.Getenv(envName) != "":
		raw = os.Getenv(envName)
	default:
		return nil, errors.New("no cluster key: set --cluster-key-file or " + envName)
	}
	key := []byte(strings.TrimSpace(raw))
	if len(key) < MinKeyLen {
		return nil, fmt.Errorf("cluster key must be at least %d characters", MinKeyLen)
	}
	return key, nil
}

func (a *Authenticator) mac(m raft.Message, body []byte, ts int64, nonce string) []byte {
	h := hmac.New(sha256.New, a.key)
	sum := sha256.Sum256(body)
	_, _ = fmt.Fprintf(h, "raftkv-peer-v1\n%d\n%d\n%d\n%d\n%x\n%d\n%s", m.From, m.To, m.Term, m.Type, sum, ts, nonce)
	return h.Sum(nil)
}

// Sign adds authentication headers for message m, whose JSON encoding is body.
func (a *Authenticator) Sign(h http.Header, m raft.Message, body []byte) error {
	var n [16]byte
	if _, err := rand.Read(n[:]); err != nil {
		return err
	}
	nonce := hex.EncodeToString(n[:])
	ts := a.now().UnixMilli()
	h.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	h.Set(HeaderNonce, nonce)
	h.Set(HeaderMAC, hex.EncodeToString(a.mac(m, body, ts, nonce)))
	return nil
}

// Verify checks the headers against message m and its raw body. The nonce is
// recorded only after the MAC is valid, so unauthenticated traffic cannot fill
// the replay cache.
func (a *Authenticator) Verify(h http.Header, m raft.Message, body []byte) error {
	tsStr, nonce, macStr := h.Get(HeaderTimestamp), h.Get(HeaderNonce), h.Get(HeaderMAC)
	if tsStr == "" || nonce == "" || macStr == "" || len(nonce) > 64 {
		return ErrMissing
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return ErrMissing
	}
	got, err := hex.DecodeString(macStr)
	if err != nil || !hmac.Equal(got, a.mac(m, body, ts, nonce)) {
		return ErrBadMAC
	}
	now := a.now()
	if d := now.Sub(time.UnixMilli(ts)); d > a.skew || d < -a.skew {
		return ErrStale
	}
	return a.remember(nonce, now)
}

func (a *Authenticator) remember(nonce string, now time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := 0
	for i < len(a.order) && !a.order[i].expires.After(now) {
		delete(a.seen, a.order[i].nonce)
		i++
	}
	a.order = a.order[i:]
	if _, dup := a.seen[nonce]; dup {
		return ErrReplay
	}
	if len(a.seen) >= a.max {
		return ErrCacheFull
	}
	a.seen[nonce] = struct{}{}
	// A timestamp can be up to one skew in the future, so the message stays
	// acceptable for up to two skews from now.
	a.order = append(a.order, seenNonce{nonce: nonce, expires: now.Add(2 * a.skew)})
	return nil
}
