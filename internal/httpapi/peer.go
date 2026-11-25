package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FrancisWorld/raftkv/internal/raft"
)

const (
	peerQueueSize = 1024
	peerTimeout   = 5 * time.Second
	peerPath      = "/raft/v1/message"
)

// PeerTransport delivers Raft messages to peers over HTTP. Each peer has a
// bounded queue drained by its own goroutine, so a slow or dead peer never
// blocks the Raft loop: when the queue is full, messages are dropped, which
// Raft tolerates by design.
type PeerTransport struct {
	log    *slog.Logger
	client *http.Client
	queues map[uint64]*peerQueue

	Dropped atomic.Uint64 // messages discarded because a peer queue was full
	Failed  atomic.Uint64 // messages whose HTTP delivery failed

	wg sync.WaitGroup
}

type peerQueue struct {
	url string
	ch  chan raft.Message
}

// NewPeerTransport prepares queues for the given peers (id -> base URL, self
// excluded). Call Start to begin sending.
func NewPeerTransport(peers map[uint64]string, log *slog.Logger) *PeerTransport {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	t := &PeerTransport{
		log:    log,
		client: &http.Client{Timeout: peerTimeout},
		queues: make(map[uint64]*peerQueue, len(peers)),
	}
	for id, base := range peers {
		t.queues[id] = &peerQueue{url: base + peerPath, ch: make(chan raft.Message, peerQueueSize)}
	}
	return t
}

// Start launches one sender goroutine per peer; they stop when ctx ends.
func (t *PeerTransport) Start(ctx context.Context) {
	for id, q := range t.queues {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.drain(ctx, id, q)
		}()
	}
}

// Wait blocks until every sender goroutine has exited.
func (t *PeerTransport) Wait() { t.wg.Wait() }

// Send implements node.Transport. It never blocks.
func (t *PeerTransport) Send(m raft.Message) {
	q, ok := t.queues[m.To]
	if !ok {
		return
	}
	select {
	case q.ch <- m:
	default:
		t.Dropped.Add(1)
	}
}

func (t *PeerTransport) drain(ctx context.Context, id uint64, q *peerQueue) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-q.ch:
			if err := t.post(ctx, q.url, m); err != nil {
				t.Failed.Add(1)
				t.log.Debug("peer send failed", "peer", id, "type", m.Type.String(), "err", err)
			}
		}
	}
}

func (t *PeerTransport) post(ctx context.Context, url string, m raft.Message) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("peer answered %s", resp.Status)
	}
	return nil
}
