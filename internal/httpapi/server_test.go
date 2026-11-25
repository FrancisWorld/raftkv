package httpapi_test

import (
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
	"github.com/FrancisWorld/raftkv/internal/raft"
)

type testNode struct {
	id     uint64
	url    string
	node   *node.Node
	srv    *http.Server
	cancel context.CancelFunc
}

func (n *testNode) stop() {
	n.cancel()
	_ = n.srv.Close()
	_ = n.node.Close()
}

// startCluster runs three real nodes in-process, talking over loopback HTTP.
func startCluster(t *testing.T) []*testNode {
	t.Helper()
	const size = 3
	listeners := make([]net.Listener, size)
	peers := map[uint64]string{}
	var ids []uint64
	for i := 0; i < size; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
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
		tr := httpapi.NewPeerTransport(others, nil)
		store := kv.NewStore()
		n, err := node.New(node.Config{
			Raft:    raft.Config{ID: id, Peers: ids, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true, Seed: uint64(id)},
			Storage: raft.NewMemStorage(), SM: store, Transport: tr, SnapshotThreshold: 20,
		})
		if err != nil {
			t.Fatal(err)
		}
		tr.Start(ctx)
		go n.RunTicker(ctx, 10*time.Millisecond)
		srv := &http.Server{Handler: httpapi.New(httpapi.Config{Node: n, Store: store, Peers: peers, Transport: tr, RequestTimeout: 3 * time.Second}).Handler()}
		go func() { _ = srv.Serve(ln) }()
		nodes = append(nodes, &testNode{id: id, url: peers[id], node: n, srv: srv, cancel: cancel})
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
	resp, err := hc.Get(follower.url + "/v1/kv/some/key?x=1")
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
	req, err := http.NewRequest(method, url, strings.NewReader(body))
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
		"not json":       `nope`,
		"unknown sender": `{"type":4,"from":99,"to":1,"term":1}`,
		"bad type":       `{"type":77,"from":2,"to":1,"term":1}`,
		"wrong target":   `{"type":4,"from":2,"to":3,"term":1}`,
	}
	for name, body := range cases {
		if code, out := do(t, "POST", n.url+"/raft/v1/message", nil, body); code != http.StatusBadRequest {
			t.Errorf("%s: status %d (%s)", name, code, out)
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
