package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func fast(c *Client) { c.backoff = time.Millisecond }

func TestFollowsRedirectAndRemembersLeader(t *testing.T) {
	var leaderHits atomic.Int32
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaderHits.Add(1)
		_, _ = w.Write([]byte(`{"key":"k","value":"v"}`))
	}))
	defer leader.Close()
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", leader.URL+r.URL.RequestURI())
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer follower.Close()

	c, err := New([]string{follower.URL}, fast)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if v, found, err := c.Get(context.Background(), "a/b c"); err != nil || !found || v != "v" {
			t.Fatalf("get = %q %v %v", v, found, err)
		}
	}
	if leaderHits.Load() != 2 {
		t.Fatalf("leader hits = %d", leaderHits.Load())
	}
}

func TestRetriesWriteWithSameSequenceAfterUnavailable(t *testing.T) {
	var seqs []string
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seqs = append(seqs, r.Header.Get("Raftkv-Seq")+"/"+r.Header.Get("Raftkv-Client-Id"))
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"leadership_lost"}`))
			return
		}
		_, _ = w.Write([]byte(`{"applied":true,"duplicate":true}`))
	}))
	defer srv.Close()
	c, _ := New([]string{srv.URL}, fast, WithClientID(77))
	res, err := c.Put(context.Background(), "k", "v")
	if err != nil || !res.Applied || !res.Duplicate {
		t.Fatalf("put = %+v %v", res, err)
	}
	if len(seqs) != 3 || seqs[0] != seqs[1] || seqs[1] != seqs[2] || seqs[0] != "1/77" {
		t.Fatalf("attempts did not reuse (seq, client): %v", seqs)
	}
	if _, err := c.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if seqs[3] != "2/77" {
		t.Fatalf("next operation should use seq 2, got %v", seqs[3])
	}
}

func TestRotatesPastDeadEndpointAndGivesUp(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"applied":true}`))
	}))
	defer good.Close()
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	c, _ := New([]string{deadURL, good.URL}, fast)
	if res, err := c.CreateIfAbsent(context.Background(), "k", "v"); err != nil || !res.Applied {
		t.Fatalf("did not fail over to the live endpoint: %+v %v", res, err)
	}

	c2, _ := New([]string{deadURL}, fast, WithMaxAttempts(3))
	if _, err := c2.CompareAndSwap(context.Background(), "k", "a", "b"); err == nil {
		t.Fatal("expected an error after exhausting attempts")
	}
}

func TestDefinitiveErrorsAreNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_key","message":"nope"}`))
	}))
	defer srv.Close()
	c, _ := New([]string{srv.URL}, fast)
	_, err := c.Put(context.Background(), "k", "v")
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != 400 || ae.Code != "invalid_key" || calls.Load() != 1 {
		t.Fatalf("err = %v, calls = %d", err, calls.Load())
	}
	if ae.Error() == "" {
		t.Fatal("empty error text")
	}
}

func TestContextCancellationStopsRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c, _ := New([]string{srv.URL}, WithBackoff(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, _, err := c.Get(ctx, "k"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":3,"role":"leader","term":2}`))
	}))
	defer srv.Close()
	c, _ := New([]string{srv.URL})
	st, err := c.Status(context.Background(), srv.URL+"/")
	if err != nil || st.ID != 3 || st.Role != "leader" {
		t.Fatalf("status = %+v %v", st, err)
	}
	if _, err := c.Status(context.Background(), "http://127.0.0.1:1"); err == nil {
		t.Fatal("expected an error for an unreachable endpoint")
	}
}
