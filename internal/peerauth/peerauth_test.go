package peerauth

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/FrancisWorld/raftkv/internal/raft"
)

var testKey = []byte(strings.Repeat("k", 40))

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func setup(t *testing.T) (*Authenticator, *clock) {
	t.Helper()
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	a, err := New(testKey, Options{Now: c.now, MaxNonces: 4})
	if err != nil {
		t.Fatal(err)
	}
	return a, c
}

func msg() (raft.Message, []byte) {
	return raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: 3, Index: 9}, []byte(`{"type":6,"from":2,"to":1,"term":3,"index":9}`)
}

func signed(t *testing.T, a *Authenticator) (http.Header, raft.Message, []byte) {
	t.Helper()
	m, body := msg()
	h := http.Header{}
	if err := a.Sign(h, m, body); err != nil {
		t.Fatal(err)
	}
	return h, m, body
}

func TestValidMessageVerifies(t *testing.T) {
	a, _ := setup(t)
	h, m, body := signed(t, a)
	if err := a.Verify(h, m, body); err != nil {
		t.Fatal(err)
	}
}

func TestForgedSenderTermTypeOrTargetIsRejected(t *testing.T) {
	a, _ := setup(t)
	for name, mutate := range map[string]func(*raft.Message){
		"from": func(m *raft.Message) { m.From = 3 },
		"to":   func(m *raft.Message) { m.To = 2 },
		"term": func(m *raft.Message) { m.Term = 99 },
		"type": func(m *raft.Message) { m.Type = raft.MsgApp },
	} {
		h, m, body := signed(t, a)
		mutate(&m)
		if err := a.Verify(h, m, body); !errors.Is(err, ErrBadMAC) {
			t.Errorf("%s: err = %v, want ErrBadMAC", name, err)
		}
	}
}

func TestTamperedBodyIsRejected(t *testing.T) {
	a, _ := setup(t)
	h, m, body := signed(t, a)
	tampered := []byte(strings.Replace(string(body), `"index":9`, `"index":900`, 1))
	if err := a.Verify(h, m, tampered); !errors.Is(err, ErrBadMAC) {
		t.Fatalf("err = %v", err)
	}
}

func TestWrongKeyAndMissingHeadersAreRejected(t *testing.T) {
	a, _ := setup(t)
	other, _ := New([]byte(strings.Repeat("z", 40)), Options{})
	h, m, body := signed(t, other)
	if err := a.Verify(h, m, body); !errors.Is(err, ErrBadMAC) {
		t.Fatalf("wrong key: %v", err)
	}
	if err := a.Verify(http.Header{}, m, body); !errors.Is(err, ErrMissing) {
		t.Fatalf("no headers: %v", err)
	}
	h.Set(HeaderTimestamp, "not-a-number")
	if err := a.Verify(h, m, body); !errors.Is(err, ErrMissing) {
		t.Fatalf("bad timestamp: %v", err)
	}
	h.Set(HeaderMAC, "zz")
	h.Set(HeaderTimestamp, "1")
	if err := a.Verify(h, m, body); !errors.Is(err, ErrBadMAC) {
		t.Fatalf("non-hex MAC: %v", err)
	}
}

func TestReplayIsRejected(t *testing.T) {
	a, _ := setup(t)
	h, m, body := signed(t, a)
	if err := a.Verify(h, m, body); err != nil {
		t.Fatal(err)
	}
	if err := a.Verify(h, m, body); !errors.Is(err, ErrReplay) {
		t.Fatalf("replay: err = %v, want ErrReplay", err)
	}
}

func TestStaleAndFutureTimestampsAreRejected(t *testing.T) {
	a, c := setup(t)
	h, m, body := signed(t, a)
	c.t = c.t.Add(31 * time.Second)
	if err := a.Verify(h, m, body); !errors.Is(err, ErrStale) {
		t.Fatalf("stale: %v", err)
	}
	c.t = c.t.Add(-62 * time.Second) // now the message is 31s in the future
	if err := a.Verify(h, m, body); !errors.Is(err, ErrStale) {
		t.Fatalf("future: %v", err)
	}
	c.t = c.t.Add(31 * time.Second)
	if err := a.Verify(h, m, body); err != nil {
		t.Fatalf("within skew: %v", err)
	}
}

func TestReplayCacheIsBoundedAndExpires(t *testing.T) {
	a, c := setup(t) // capacity 4
	for i := 0; i < 4; i++ {
		h, m, body := signed(t, a)
		if err := a.Verify(h, m, body); err != nil {
			t.Fatal(err)
		}
	}
	h, m, body := signed(t, a)
	if err := a.Verify(h, m, body); !errors.Is(err, ErrCacheFull) {
		t.Fatalf("full cache: %v", err)
	}
	c.t = c.t.Add(61 * time.Second) // everything recorded has aged out
	h, m, body = signed(t, a)
	if err := a.Verify(h, m, body); err != nil {
		t.Fatalf("after expiry: %v", err)
	}
	if len(a.seen) != 1 {
		t.Fatalf("expired nonces not purged: %d", len(a.seen))
	}
}

func TestUnauthenticatedTrafficCannotFillTheCache(t *testing.T) {
	a, _ := setup(t)
	m, body := msg()
	for i := 0; i < 20; i++ {
		h := http.Header{}
		h.Set(HeaderTimestamp, "1700000000000")
		h.Set(HeaderNonce, string(rune('a'+i)))
		h.Set(HeaderMAC, "00")
		_ = a.Verify(h, m, body)
	}
	if len(a.seen) != 0 {
		t.Fatalf("forged messages were recorded: %d", len(a.seen))
	}
}

func TestKeyValidationAndLoading(t *testing.T) {
	if _, err := New([]byte("short"), Options{}); err == nil {
		t.Fatal("short key accepted")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "key")
	if err := os.WriteFile(p, []byte(string(testKey)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	k, err := LoadKey(p, "RAFTKV_TEST_KEY_UNSET")
	if err != nil || string(k) != string(testKey) {
		t.Fatalf("file key = %q %v", k, err)
	}
	t.Setenv("RAFTKV_TEST_KEY", string(testKey))
	if k, err := LoadKey("", "RAFTKV_TEST_KEY"); err != nil || len(k) != len(testKey) {
		t.Fatalf("env key: %v", err)
	}
	if _, err := LoadKey("", "RAFTKV_TEST_KEY_UNSET"); err == nil {
		t.Fatal("missing key accepted")
	}
	if err := os.WriteFile(p, []byte("tiny"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKey(p, ""); err == nil {
		t.Fatal("short file key accepted")
	}
	if _, err := LoadKey(filepath.Join(dir, "nope"), ""); err == nil {
		t.Fatal("missing file accepted")
	}
}
