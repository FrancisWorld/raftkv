package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/FrancisWorld/raftkv/client"
	"github.com/FrancisWorld/raftkv/internal/httpapi"
	"github.com/FrancisWorld/raftkv/internal/kv"
	"github.com/FrancisWorld/raftkv/internal/node"
	"github.com/FrancisWorld/raftkv/internal/peerauth"
	"github.com/FrancisWorld/raftkv/internal/raft"
)

type testNode struct {
	id         uint64
	url        string
	node       *node.Node
	srv        *http.Server
	cancel     context.CancelFunc
	stopTicker context.CancelFunc
}

var clusterKey = []byte(strings.Repeat("c", 40))

func (n *testNode) stop() {
	n.cancel()
	n.stopTicker()
	_ = n.srv.Close()
	_ = n.node.Close()
}

// startCluster runs three real nodes in-process, talking over loopback HTTP.
func startCluster(t *testing.T) []*testNode { return startClusterAuth(t, clusterKey) }

// startClusterAuth is startCluster with a chosen cluster key; nil disables peer
// authentication (the --insecure-peer mode).
func startClusterAuth(t *testing.T, key []byte) []*testNode {
	t.Helper()
	const size = 3
	listeners := make([]net.Listener, size)
	peers := map[uint64]string{}
	var ids []uint64
	for i := 0; i < size; i++ {
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		id := uint64(i + 1)
		ids = append(ids, id)
		peers[id] = "http://" + ln.Addr().String()
	}
	var nodes []*testNode
	for i, ln := range listeners {
		id := uint64(i + 1)
		others := map[uint64]string{}
		for pid, u := range peers {
			if pid != id {
				others[pid] = u
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		var auth *peerauth.Authenticator
		if key != nil {
			var err error
			if auth, err = peerauth.New(key, peerauth.Options{}); err != nil {
				t.Fatal(err)
			}
		}
		tr := httpapi.NewPeerTransport(others, auth, nil)
		store := kv.NewStore()
		n, err := node.New(node.Config{
			Raft:    raft.Config{ID: id, Peers: ids, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true, Seed: id},
			Storage: raft.NewMemStorage(), SM: store, Transport: tr, SnapshotThreshold: 20,
		})
		if err != nil {
			t.Fatal(err)
		}
		tr.Start(ctx)
		tickCtx, stopTicker := context.WithCancel(ctx)
		go n.RunTicker(tickCtx, 10*time.Millisecond)
		srv := &http.Server{Handler: httpapi.New(httpapi.Config{Node: n, Store: store, Peers: peers, PeerAuth: auth, Transport: tr, RequestTimeout: 3 * time.Second}).Handler()}
		go func() { _ = srv.Serve(ln) }()
		nodes = append(nodes, &testNode{id: id, url: peers[id], node: n, srv: srv, cancel: cancel, stopTicker: stopTicker})
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.stop()
		}
	})
	return nodes
}

func endpoints(nodes []*testNode) []string {
	var out []string
	for _, n := range nodes {
		out = append(out, n.url)
	}
	return out
}

func waitLeader(t *testing.T, nodes []*testNode) *testNode {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.node.Err() == nil && n.node.Status().Role == raft.Leader {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}

func newClient(t *testing.T, eps []string) *client.Client {
	t.Helper()
	c, err := client.New(eps, client.WithBackoff(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestClientEndToEndThroughAFollower(t *testing.T) {
	nodes := startCluster(t)
	leader := waitLeader(t, nodes)
	var follower *testNode
	for _, n := range nodes {
		if n != leader {
			follower = n
		}
	}
	c := newClient(t, []string{follower.url}) // forces a redirect to the leader
	cx := ctx(t)

	if _, found, err := c.Get(cx, "missing"); err != nil || found {
		t.Fatalf("get missing: found=%v err=%v", found, err)
	}
	if res, err := c.Put(cx, "greeting/en", "hello"); err != nil || !res.Applied || res.PrevFound {
		t.Fatalf("put: %+v %v", res, err)
	}
	if v, found, err := c.Get(cx, "greeting/en"); err != nil || !found || v != "hello" {
		t.Fatalf("get: %q %v %v", v, found, err)
	}
	if res, err := c.CompareAndSwap(cx, "greeting/en", "nope", "x"); err != nil || res.Applied || res.Prev != "hello" {
		t.Fatalf("failing cas: %+v %v", res, err)
	}
	if res, err := c.CompareAndSwap(cx, "greeting/en", "hello", "olá"); err != nil || !res.Applied {
		t.Fatalf("cas: %+v %v", res, err)
	}
	if res, err := c.CreateIfAbsent(cx, "greeting/en", "again"); err != nil || res.Applied {
		t.Fatalf("create-if-absent on an existing key: %+v %v", res, err)
	}
	if v, _, _ := c.Get(cx, "greeting/en"); v != "olá" {
		t.Fatalf("value after cas = %q", v)
	}
	if res, err := c.Delete(cx, "greeting/en"); err != nil || !res.Applied {
		t.Fatalf("delete: %+v %v", res, err)
	}
	if _, found, _ := c.Get(cx, "greeting/en"); found {
		t.Fatal("key survived delete")
	}
	st, err := c.Status(cx, leader.url)
	if err != nil || st.Role != "leader" || st.ID != leader.id {
		t.Fatalf("status = %+v %v", st, err)
	}
}

func TestFollowerRedirectsToLeaderWith307(t *testing.T) {
	nodes := startCluster(t)
	leader := waitLeader(t, nodes)
	// Followers need a heartbeat before they know who leads.
	time.Sleep(300 * time.Millisecond)
	var follower *testNode
	for _, n := range nodes {
		if n != leader {
			follower = n
		}
	}
	hc := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, follower.url+"/v1/kv/some/key?x=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != leader.url+"/v1/kv/some/key?x=1" {
		t.Fatalf("Location = %q, want the leader URL with path and query preserved", loc)
	}
}

func do(t *testing.T, method, url string, hdr map[string]string, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestWireLevelExactlyOnceAndValidation(t *testing.T) {
	nodes := startCluster(t)
	leader := waitLeader(t, nodes)
	hdr := map[string]string{httpapi.HeaderClientID: "42", httpapi.HeaderSeq: "1"}
	body := `{"expect_absent":true,"value":"v"}`

	code, first := do(t, "POST", leader.url+"/v1/cas/ticket", hdr, body)
	if code != 200 || !strings.Contains(first, `"applied":true`) || strings.Contains(first, "duplicate") {
		t.Fatalf("first: %d %s", code, first)
	}
	// The retry would fail its precondition if it were executed again.
	code, retry := do(t, "POST", leader.url+"/v1/cas/ticket", hdr, body)
	if code != 200 || !strings.Contains(retry, `"applied":true`) || !strings.Contains(retry, `"duplicate":true`) {
		t.Fatalf("retry: %d %s", code, retry)
	}
	// An older sequence number from the same client is refused, not executed.
	do(t, "PUT", leader.url+"/v1/kv/other", map[string]string{httpapi.HeaderClientID: "42", httpapi.HeaderSeq: "2"}, `{"value":"x"}`)
	if code, out := do(t, "PUT", leader.url+"/v1/kv/other", hdr, `{"value":"y"}`); code != http.StatusConflict {
		t.Fatalf("stale seq: %d %s", code, out)
	}

	bad := []struct {
		name, method, path string
		hdr                map[string]string
		body               string
	}{
		{"bad client id", "PUT", "/v1/kv/k", map[string]string{httpapi.HeaderClientID: "abc", httpapi.HeaderSeq: "1"}, `{"value":"x"}`},
		{"missing seq", "PUT", "/v1/kv/k", map[string]string{httpapi.HeaderClientID: "5"}, `{"value":"x"}`},
		{"unknown field", "PUT", "/v1/kv/k", nil, `{"valeu":"x"}`},
		{"not json", "PUT", "/v1/kv/k", nil, `value=x`},
		{"empty key", "PUT", "/v1/kv/", nil, `{"value":"x"}`},
		{"oversized key", "PUT", "/v1/kv/" + strings.Repeat("k", kv.MaxKeyLen+1), nil, `{"value":"x"}`},
	}
	for _, b := range bad {
		if code, out := do(t, b.method, leader.url+b.path, b.hdr, b.body); code != http.StatusBadRequest && code != http.StatusNotFound {
			t.Errorf("%s: status %d (%s), want 400", b.name, code, out)
		}
	}
}

func TestSurvivesLeaderFailure(t *testing.T) {
	nodes := startCluster(t)
	leader := waitLeader(t, nodes)
	c := newClient(t, endpoints(nodes))
	cx := ctx(t)
	for i := 0; i < 30; i++ { // enough to trigger snapshots (threshold 20)
		if _, err := c.Put(cx, fmt.Sprintf("k%d", i), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	leader.stop()
	if _, err := c.Put(cx, "after-failover", "ok"); err != nil {
		t.Fatalf("write after leader failure: %v", err)
	}
	for _, k := range []string{"k0", "k29", "after-failover"} {
		if _, found, err := c.Get(cx, k); err != nil || !found {
			t.Fatalf("key %s lost: found=%v err=%v", k, found, err)
		}
	}
}

func TestPeerEndpointRejectsGarbage(t *testing.T) {
	nodes := startCluster(t)
	n := nodes[0]
	cases := map[string]string{
		"not json": `nope`,
	}
	for name, body := range cases {
		if code, out := do(t, "POST", n.url+"/raft/v1/message", nil, body); code != http.StatusBadRequest {
			t.Errorf("%s: status %d (%s)", name, code, out)
		}
	}
	// Well-formed but unsigned messages are refused before any validation.
	for name, body := range map[string]string{
		"unknown sender": `{"type":4,"from":99,"to":1,"term":1}`,
		"bad type":       `{"type":77,"from":2,"to":1,"term":1}`,
		"wrong target":   `{"type":4,"from":2,"to":3,"term":1}`,
	} {
		if code, out := do(t, "POST", n.url+"/raft/v1/message", nil, body); code != http.StatusUnauthorized {
			t.Errorf("%s: status %d (%s), want 401", name, code, out)
		}
	}
}

func TestMetricsAndStatusEndpoints(t *testing.T) {
	nodes := startCluster(t)
	leader := waitLeader(t, nodes)
	c := newClient(t, []string{leader.url})
	if _, err := c.Put(ctx(t), "m", "1"); err != nil {
		t.Fatal(err)
	}
	code, out := do(t, "GET", leader.url+"/metrics", nil, "")
	if code != 200 {
		t.Fatalf("metrics status %d", code)
	}
	for _, want := range []string{
		"raftkv_raft_is_leader 1", "raftkv_raft_term ", "raftkv_raft_commit_index ",
		`raftkv_http_requests_total{route="put",code="200"} 1`, "raftkv_request_duration_seconds_count 1",
		"raftkv_peer_messages_dropped_total",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	code, out = do(t, "GET", leader.url+"/v1/status", nil, "")
	var st map[string]any
	if code != 200 || json.Unmarshal([]byte(out), &st) != nil || st["role"] != "leader" {
		t.Fatalf("status: %d %s", code, out)
	}
	if code, _ := do(t, "GET", leader.url+"/healthz", nil, ""); code != http.StatusNoContent {
		t.Fatalf("healthz = %d", code)
	}
	if code, _ := do(t, "GET", leader.url+"/readyz", nil, ""); code != http.StatusNoContent {
		t.Fatalf("readyz = %d", code)
	}
}

func TestClientRejectsBadEndpoints(t *testing.T) {
	if _, err := client.New(nil); err == nil {
		t.Fatal("expected an error for no endpoints")
	}
	if _, err := client.New([]string{"not a url"}); err == nil {
		t.Fatal("expected an error for a malformed endpoint")
	}
}

// signedPost sends msg to url with valid authentication headers for key.
func signedPost(t *testing.T, key []byte, url string, m raft.Message, mutateBody func([]byte) []byte) (int, http.Header) {
	t.Helper()
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	hdr := http.Header{}
	if key != nil {
		a, err := peerauth.New(key, peerauth.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Sign(hdr, m, body); err != nil {
			t.Fatal(err)
		}
	}
	if mutateBody != nil {
		body = mutateBody(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url+"/raft/v1/message", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header = hdr
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode, resp.Header
}

// isolatedLeader elects a leader, freezes it (no more ticks, so it neither
// heartbeats nor steps down on check-quorum) and kills both followers, leaving
// a leader that can only learn of a "commit" from forged acknowledgements.
func isolatedLeader(t *testing.T, key []byte) (*testNode, *node.Future) {
	t.Helper()
	nodes := startClusterAuth(t, key)
	leader := waitLeader(t, nodes)
	time.Sleep(200 * time.Millisecond) // let the no-op commit everywhere
	leader.stopTicker()
	for _, n := range nodes {
		if n != leader {
			n.stop()
		}
	}
	fut := leader.node.Propose(kv.Command{ClientID: 5, Seq: 1, Op: kv.OpPut, Key: "forged", Value: "never-stored"}.Encode())
	return leader, fut
}

func forgedAck(leader *testNode) raft.Message {
	st := leader.node.Status()
	from := leader.id%3 + 1 // some other node
	return raft.Message{Type: raft.MsgAppResp, From: from, To: leader.id, Term: st.Term, Index: st.LastIndex}
}

func committed(fut *node.Future, wait time.Duration) bool {
	select {
	case <-fut.Done():
		return true
	case <-time.After(wait):
		return false
	}
}

// The attack: with two followers dead, a forged acknowledgement "from" a peer
// is all a leader needs to commit an entry no follower ever stored.
func TestForgedAcksCommitNothingWhenPeerAuthIsOn(t *testing.T) {
	leader, fut := isolatedLeader(t, clusterKey)
	before := leader.node.Status().Commit
	ack := forgedAck(leader)

	// No headers, a MAC from the wrong key, and a tampered body all fail.
	if code, _ := signedPost(t, nil, leader.url, ack, nil); code != http.StatusUnauthorized {
		t.Fatalf("unsigned forgery: %d", code)
	}
	if code, _ := signedPost(t, []byte(strings.Repeat("x", 40)), leader.url, ack, nil); code != http.StatusUnauthorized {
		t.Fatalf("wrong-key forgery: %d", code)
	}
	tamper := func(b []byte) []byte {
		return bytes.Replace(b, []byte(fmt.Sprintf(`"from":%d`, ack.From)), []byte(fmt.Sprintf(`"from":%d`, ack.From%3+1)), 1)
	}
	if code, _ := signedPost(t, clusterKey, leader.url, ack, tamper); code != http.StatusUnauthorized {
		t.Fatalf("tampered forgery: %d", code)
	}
	if committed(fut, 400*time.Millisecond) || leader.node.Status().Commit != before {
		t.Fatal("a forged acknowledgement advanced the commit index")
	}
	_, scrape := do(t, "GET", leader.url+"/metrics", nil, "")
	if !strings.Contains(scrape, `raftkv_peer_auth_failures_total{reason="missing"} 1`) ||
		!strings.Contains(scrape, `reason="bad_mac"`) {
		t.Fatalf("auth failures not counted:\n%s", scrape)
	}
}

// Control: the same attack against --insecure-peer mode does commit, which is
// what the authentication closes.
func TestForgedAcksCommitWithoutPeerAuth(t *testing.T) {
	leader, fut := isolatedLeader(t, nil)
	if code, _ := signedPost(t, nil, leader.url, forgedAck(leader), nil); code != http.StatusNoContent {
		t.Fatalf("forgery status %d", code)
	}
	if !committed(fut, 3*time.Second) {
		t.Fatal("expected the forged ack to commit in insecure mode (control case)")
	}
}

func TestPeerMessageReplayAndStaleTimestampAreRejected(t *testing.T) {
	nodes := startCluster(t)
	n := nodes[0]
	a, _ := peerauth.New(clusterKey, peerauth.Options{})
	m := raft.Message{Type: raft.MsgHeartbeatResp, From: 2, To: n.id, Term: 0}
	body, _ := json.Marshal(m)
	post := func(h http.Header) int {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, n.url+"/raft/v1/message", bytes.NewReader(body))
		req.Header = h
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	h := http.Header{}
	_ = a.Sign(h, m, body)
	if code := post(h); code != http.StatusNoContent {
		t.Fatalf("first delivery: %d", code)
	}
	if code := post(h); code != http.StatusUnauthorized {
		t.Fatalf("replay: %d", code)
	}
	old, _ := peerauth.New(clusterKey, peerauth.Options{Now: func() time.Time { return time.Now().Add(-5 * time.Minute) }})
	h2 := http.Header{}
	_ = old.Sign(h2, m, body)
	if code := post(h2); code != http.StatusUnauthorized {
		t.Fatalf("stale timestamp: %d", code)
	}
}
