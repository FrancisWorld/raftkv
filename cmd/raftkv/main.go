// Command raftkv runs one node of a raftkv cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/FrancisWorld/raftkv/internal/httpapi"
	"github.com/FrancisWorld/raftkv/internal/kv"
	"github.com/FrancisWorld/raftkv/internal/node"
	"github.com/FrancisWorld/raftkv/internal/raft"
	"github.com/FrancisWorld/raftkv/internal/wal"
)

type options struct {
	id                uint64
	listen            string
	peers             string
	clientURLs        string
	dataDir           string
	tick              time.Duration
	electionTicks     int
	heartbeatTicks    int
	snapshotThreshold uint64
	requestTimeout    time.Duration
	logLevel          string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "raftkv:", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("raftkv", flag.ContinueOnError)
	fs.Uint64Var(&o.id, "id", 0, "this node's id (must appear in --peers)")
	fs.StringVar(&o.listen, "listen", ":8080", "address to listen on for clients and peers")
	fs.StringVar(&o.peers, "peers", "", "cluster members as id=url pairs, e.g. 1=http://n1:8080,2=http://n2:8080,3=http://n3:8080")
	fs.StringVar(&o.clientURLs, "client-urls", "", "id=url pairs clients use to reach each node, if different from --peers (used for leader redirects)")
	fs.StringVar(&o.dataDir, "data-dir", "./data", "directory for the write-ahead log and snapshots")
	fs.DurationVar(&o.tick, "tick", 100*time.Millisecond, "length of one Raft tick")
	fs.IntVar(&o.electionTicks, "election-ticks", 10, "base election timeout in ticks (randomised to 1x-2x)")
	fs.IntVar(&o.heartbeatTicks, "heartbeat-ticks", 1, "heartbeat interval in ticks")
	fs.Uint64Var(&o.snapshotThreshold, "snapshot-threshold", 10000, "applied entries between snapshots (0 disables compaction)")
	fs.DurationVar(&o.requestTimeout, "request-timeout", 5*time.Second, "how long a client request waits for consensus")
	fs.StringVar(&o.logLevel, "log-level", "info", "debug, info, warn or error")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if o.id == 0 {
		return o, errors.New("--id is required")
	}
	if o.tick <= 0 || o.requestTimeout <= 0 {
		return o, errors.New("--tick and --request-timeout must be positive")
	}
	return o, nil
}

// parsePeers parses "1=http://a:1,2=http://b:2" into a map of trimmed base URLs.
func parsePeers(s string) (map[uint64]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, errors.New("a peer list is required")
	}
	return parseURLMap(s)
}

func parseURLMap(s string) (map[uint64]string, error) {
	peers := map[uint64]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, url, ok := strings.Cut(part, "=")
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
		if !ok || err != nil || id == 0 || !strings.HasPrefix(url, "http") {
			return nil, fmt.Errorf("invalid peer %q (want id=http://host:port)", part)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("duplicate peer id %d", id)
		}
		peers[id] = strings.TrimRight(url, "/")
	}
	return peers, nil
}

func newLogger(level string) (*slog.Logger, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid --log-level %q", level)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})), nil
}

func run(args []string) error {
	o, err := parseFlags(args)
	if err != nil {
		return err
	}
	peers, err := parsePeers(o.peers)
	if err != nil {
		return err
	}
	if _, ok := peers[o.id]; !ok {
		return fmt.Errorf("--id %d is not listed in --peers", o.id)
	}
	var clientURLs map[uint64]string
	if o.clientURLs != "" {
		if clientURLs, err = parseURLMap(o.clientURLs); err != nil {
			return fmt.Errorf("--client-urls: %w", err)
		}
	}
	log, err := newLogger(o.logLevel)
	if err != nil {
		return err
	}

	ids := make([]uint64, 0, len(peers))
	others := map[uint64]string{}
	for id, u := range peers {
		ids = append(ids, id)
		if id != o.id {
			others[id] = u
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	storage, err := wal.Open(filepath.Join(o.dataDir, fmt.Sprintf("node-%d", o.id)))
	if err != nil {
		return fmt.Errorf("open data dir: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	transport := httpapi.NewPeerTransport(others, log)
	store := kv.NewStore()
	n, err := node.New(node.Config{
		Raft: raft.Config{
			ID: o.id, Peers: ids, ElectionTick: o.electionTicks, HeartbeatTick: o.heartbeatTicks,
			PreVote: true, CheckQuorum: true, Seed: uint64(time.Now().UnixNano()),
		},
		Storage: storage, SM: store, Transport: transport,
		SnapshotThreshold: o.snapshotThreshold, Logger: log,
	})
	if err != nil {
		_ = storage.Close()
		return err
	}

	srv := &http.Server{
		Addr:              o.listen,
		Handler:           httpapi.New(httpapi.Config{Node: n, Store: store, Peers: peers, ClientURLs: clientURLs, Transport: transport, RequestTimeout: o.requestTimeout, Logger: log}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	transport.Start(ctx)
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		n.RunTicker(ctx, o.tick)
	}()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	log.Info("raftkv listening", "id", o.id, "addr", o.listen, "peers", len(peers))

	select {
	case err = <-serveErr:
	case <-ctx.Done():
		log.Info("shutting down")
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if sdErr := srv.Shutdown(shutdownCtx); sdErr != nil && err == nil {
		err = sdErr
	}
	<-tickDone
	transport.Wait()
	if closeErr := n.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	if fatal := n.Err(); fatal != nil && err == nil {
		err = fatal
	}
	return err
}
