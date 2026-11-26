// Package httpapi exposes a node over HTTP: the client key-value API, a status
// endpoint, Prometheus metrics, and the peer endpoint Raft messages arrive on.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/FrancisWorld/raftkv/internal/kv"
	"github.com/FrancisWorld/raftkv/internal/metrics"
	"github.com/FrancisWorld/raftkv/internal/node"
	"github.com/FrancisWorld/raftkv/internal/peerauth"
	"github.com/FrancisWorld/raftkv/internal/raft"
)

// Header names carrying the client session identity of a write.
const (
	HeaderClientID = "Raftkv-Client-Id"
	HeaderSeq      = "Raftkv-Seq"
	HeaderLeader   = "Raftkv-Leader"
)

const maxBody = kv.MaxValueLen*2 + 4096

// maxPeerBody bounds a Raft message, which may carry a whole snapshot.
const maxPeerBody = 512 << 20

// Config configures a Server.
type Config struct {
	Node  *node.Node
	Store *kv.Store
	// Peers maps every node id (self included) to the base URL peers use to
	// reach it. It only validates that a message names a known node; it proves
	// nothing about the caller, which is what PeerAuth is for.
	Peers map[uint64]string
	// PeerAuth authenticates /raft/v1/message requests. When nil the endpoint
	// accepts unauthenticated messages, which is only safe on a trusted network
	// (the --insecure-peer development mode).
	PeerAuth *peerauth.Authenticator
	// ClientURLs maps node ids to the base URL clients use, which can differ from
	// the peer URL (for example behind Docker networking). Leader redirects use
	// it. When nil, Peers is used.
	ClientURLs map[uint64]string
	// RequestTimeout bounds how long a request waits for consensus.
	RequestTimeout time.Duration
	Transport      *PeerTransport
	Logger         *slog.Logger
}

// Server is the HTTP front end of one node.
type Server struct {
	cfg      Config
	log      *slog.Logger
	reg      *metrics.Registry
	requests *metrics.CounterVec
	latency  *metrics.Histogram
	authFail *metrics.CounterVec
}

// New builds a Server and registers its metrics.
func New(cfg Config) *Server {
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.ClientURLs == nil {
		cfg.ClientURLs = cfg.Peers
	}
	s := &Server{cfg: cfg, log: cfg.Logger, reg: metrics.NewRegistry()}
	s.requests = s.reg.NewCounterVec("raftkv_http_requests_total", "Client API requests by route and status code.", "route", "code")
	s.latency = s.reg.NewHistogram("raftkv_request_duration_seconds", "Time to serve client key-value requests.",
		[]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})
	s.authFail = s.reg.NewCounterVec("raftkv_peer_auth_failures_total", "Rejected peer messages by reason.", "reason")
	s.registerNodeMetrics()
	return s
}

func (s *Server) registerNodeMetrics() {
	st := func() node.Status { return s.cfg.Node.Status() }
	g := func(name, help string, f func(node.Status) float64) {
		s.reg.NewGaugeFunc(name, help, func() float64 { return f(st()) })
	}
	c := func(name, help string, f func(node.Status) float64) {
		s.reg.NewCounterFunc(name, help, func() float64 { return f(st()) })
	}
	g("raftkv_raft_term", "Current Raft term.", func(s node.Status) float64 { return float64(s.Term) })
	g("raftkv_raft_is_leader", "1 if this node is the leader.", func(s node.Status) float64 {
		if s.Role == raft.Leader {
			return 1
		}
		return 0
	})
	g("raftkv_raft_commit_index", "Highest committed log index.", func(s node.Status) float64 { return float64(s.Commit) })
	g("raftkv_raft_applied_index", "Highest applied log index.", func(s node.Status) float64 { return float64(s.Applied) })
	g("raftkv_raft_last_index", "Last log index.", func(s node.Status) float64 { return float64(s.LastIndex) })
	g("raftkv_raft_snapshot_index", "Log index covered by the latest snapshot.", func(s node.Status) float64 { return float64(s.SnapshotIndex) })
	g("raftkv_keys", "Number of keys in the store.", func(node.Status) float64 { return float64(s.cfg.Store.Len()) })
	c("raftkv_raft_proposals_total", "Commands proposed on this node.", func(s node.Status) float64 { return float64(s.Stats.Proposals) })
	c("raftkv_raft_applied_entries_total", "Commands applied to the state machine.", func(s node.Status) float64 { return float64(s.Stats.AppliedEntries) })
	c("raftkv_raft_snapshots_taken_total", "Log compactions performed.", func(s node.Status) float64 { return float64(s.Stats.SnapshotsTaken) })
	c("raftkv_raft_snapshots_installed_total", "Snapshots received from a leader.", func(s node.Status) float64 { return float64(s.Stats.SnapshotsInstalled) })
	c("raftkv_raft_messages_sent_total", "Raft messages handed to the transport.", func(s node.Status) float64 { return float64(s.Stats.MessagesSent) })
	c("raftkv_raft_messages_received_total", "Raft messages received.", func(s node.Status) float64 { return float64(s.Stats.MessagesReceived) })
	c("raftkv_raft_read_barriers_total", "ReadIndex barriers started.", func(s node.Status) float64 { return float64(s.Stats.ReadBarriers) })
	c("raftkv_raft_leader_changes_total", "Times a new leader was observed.", func(s node.Status) float64 { return float64(s.Stats.LeaderChanges) })
	if t := s.cfg.Transport; t != nil {
		s.reg.NewCounterFunc("raftkv_peer_messages_dropped_total", "Outbound messages dropped on a full peer queue.", func() float64 { return float64(t.Dropped.Load()) })
		s.reg.NewCounterFunc("raftkv_peer_send_failures_total", "Outbound messages that failed to deliver.", func() float64 { return float64(t.Failed.Load()) })
	}
}

// Handler returns the HTTP handler serving every endpoint.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/kv/{key...}", s.instrument("get", s.handleGet))
	mux.HandleFunc("PUT /v1/kv/{key...}", s.instrument("put", s.handlePut))
	mux.HandleFunc("DELETE /v1/kv/{key...}", s.instrument("delete", s.handleDelete))
	mux.HandleFunc("POST /v1/cas/{key...}", s.instrument("cas", s.handleCAS))
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("GET /metrics", s.reg)
	mux.HandleFunc("POST "+peerPath, s.handlePeer)
	return mux
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) instrument(route string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		h(rec, r)
		s.requests.With(route, strconv.Itoa(rec.code)).Inc()
		s.latency.Observe(time.Since(start).Seconds())
	}
}

// ---- JSON helpers ----

type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, errorBody{Error: kind, Message: msg})
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// ---- client API ----

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type putRequest struct {
	Value string `json:"value"`
}

type casRequest struct {
	Expected     string `json:"expected"`
	ExpectAbsent bool   `json:"expect_absent"`
	Value        string `json:"value"`
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" || len(key) > kv.MaxKeyLen {
		writeError(w, http.StatusBadRequest, "invalid_key", fmt.Sprintf("key must be 1..%d bytes", kv.MaxKeyLen))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	// ReadIndex: leadership is confirmed by a quorum before the local state is read.
	if _, err := s.cfg.Node.ReadBarrier().Wait(ctx); err != nil {
		s.fail(w, r, err)
		return
	}
	v, ok := s.cfg.Store.Get(key)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no such key")
		return
	}
	writeJSON(w, http.StatusOK, getResponse{Key: key, Value: v})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	var req putRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	s.submit(w, r, kv.Command{Op: kv.OpPut, Key: r.PathValue("key"), Value: req.Value})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	s.submit(w, r, kv.Command{Op: kv.OpDelete, Key: r.PathValue("key")})
}

func (s *Server) handleCAS(w http.ResponseWriter, r *http.Request) {
	var req casRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	s.submit(w, r, kv.Command{Op: kv.OpCAS, Key: r.PathValue("key"), Value: req.Value, Expected: req.Expected, ExpectAbsent: req.ExpectAbsent})
}

// session reads the client's identity. Clients that omit it still work, but get
// a fresh identity per request, so their retries are not deduplicated.
func session(r *http.Request) (clientID, seq uint64, err error) {
	cid, sq := r.Header.Get(HeaderClientID), r.Header.Get(HeaderSeq)
	if cid == "" && sq == "" {
		return rand.Uint64N(1<<62) + 1, 1, nil
	}
	if clientID, err = strconv.ParseUint(cid, 10, 64); err != nil || clientID == 0 {
		return 0, 0, fmt.Errorf("%s must be a positive integer", HeaderClientID)
	}
	if seq, err = strconv.ParseUint(sq, 10, 64); err != nil || seq == 0 {
		return 0, 0, fmt.Errorf("%s must be a positive integer", HeaderSeq)
	}
	return clientID, seq, nil
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request, cmd kv.Command) {
	var err error
	if cmd.ClientID, cmd.Seq, err = session(r); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_session", err.Error())
		return
	}
	if err := cmd.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_command", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	raw, err := s.cfg.Node.Propose(cmd.Encode()).Wait(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	res, err := kv.DecodeResult(raw)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if res.Error != "" {
		writeError(w, http.StatusConflict, "stale_sequence", "a newer command from this client was already applied")
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// fail maps a node error to an HTTP response. Followers redirect to the leader
// with 307, which preserves the method and body, so a plain HTTP client can
// follow it; leadership loss and timeouts are retryable with the same Seq.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	var nl *node.NotLeaderError
	switch {
	case errors.As(err, &nl):
		if base, ok := s.cfg.ClientURLs[nl.Leader]; ok && nl.Leader != 0 {
			w.Header().Set(HeaderLeader, strconv.FormatUint(nl.Leader, 10))
			w.Header().Set("Location", base+r.URL.RequestURI())
			writeError(w, http.StatusTemporaryRedirect, "not_leader", "redirecting to the leader")
			return
		}
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "no_leader", "no leader is known yet")
	case errors.Is(err, node.ErrNotReady):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "leader_not_ready", err.Error())
	case errors.Is(err, node.ErrLeadershipLost):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "leadership_lost", "outcome unknown; retry with the same sequence number")
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "timeout", "no consensus within the request timeout; retry with the same sequence number")
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, "canceled", "request canceled")
	default:
		s.log.Error("request failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "unavailable", err.Error())
	}
}

// ---- operational endpoints ----

type statusResponse struct {
	ID            uint64 `json:"id"`
	Role          string `json:"role"`
	Term          uint64 `json:"term"`
	Leader        uint64 `json:"leader"`
	LeaderURL     string `json:"leader_url,omitempty"`
	CommitIndex   uint64 `json:"commit_index"`
	AppliedIndex  uint64 `json:"applied_index"`
	LastIndex     uint64 `json:"last_index"`
	SnapshotIndex uint64 `json:"snapshot_index"`
	Keys          int    `json:"keys"`
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	st := s.cfg.Node.Status()
	writeJSON(w, http.StatusOK, statusResponse{
		ID: st.ID, Role: st.Role.String(), Term: st.Term, Leader: st.Leader, LeaderURL: s.cfg.ClientURLs[st.Leader],
		CommitIndex: st.Commit, AppliedIndex: st.Applied, LastIndex: st.LastIndex, SnapshotIndex: st.SnapshotIndex,
		Keys: s.cfg.Store.Len(),
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	if err := s.cfg.Node.Err(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "halted", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleReady succeeds once the node knows a leader, i.e. it can serve or
// redirect requests.
func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if s.cfg.Node.Err() != nil || s.cfg.Node.Status().Leader == 0 {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "no leader known")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- peer endpoint ----

func (s *Server) handlePeer(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxPeerBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_message", err.Error())
		return
	}
	var m raft.Message
	if err := json.Unmarshal(body, &m); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_message", err.Error())
		return
	}
	if s.cfg.PeerAuth != nil {
		if err := s.cfg.PeerAuth.Verify(r.Header, m, body); err != nil {
			s.authFail.With(authReason(err)).Inc()
			s.log.Warn("rejected peer message", "reason", authReason(err), "remote", r.RemoteAddr)
			writeError(w, http.StatusUnauthorized, "unauthorized", "peer authentication failed")
			return
		}
	}
	if _, known := s.cfg.Peers[m.From]; !known || m.From == m.To || !m.Type.Valid() {
		writeError(w, http.StatusBadRequest, "invalid_message", "unknown sender, receiver or type")
		return
	}
	if m.To != s.cfg.Node.Status().ID {
		writeError(w, http.StatusBadRequest, "invalid_message", "message addressed to another node")
		return
	}
	s.cfg.Node.Receive(m)
	w.WriteHeader(http.StatusNoContent)
}

func authReason(err error) string {
	switch {
	case errors.Is(err, peerauth.ErrMissing):
		return "missing"
	case errors.Is(err, peerauth.ErrBadMAC):
		return "bad_mac"
	case errors.Is(err, peerauth.ErrStale):
		return "stale"
	case errors.Is(err, peerauth.ErrReplay):
		return "replay"
	}
	return "other"
}
