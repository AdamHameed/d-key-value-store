package raftstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/adam/d-key-value-store/internal/storage"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
)

type Peer struct {
	ID       string
	RaftAddr string
	GRPCAddr string
}

type Config struct {
	NodeID       string
	RaftAddr     string
	GRPCAddr     string
	DataDir      string
	Peers        []Peer
	ApplyTimeout time.Duration
}

type Store struct {
	raft         *raft.Raft
	fsm          *fsm
	nodeID       string
	raftAddr     string
	grpcAddr     string
	peersByID    map[string]Peer
	applyTimeout time.Duration
}

type command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

func Open(cfg Config, kv *storage.Store) (*Store, error) {
	if cfg.ApplyTimeout == 0 {
		cfg.ApplyTimeout = 5 * time.Second
	}
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "raft"), 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "snapshots"), 0755); err != nil {
		return nil, err
	}
	logEvent("recovery_start", "node", cfg.NodeID, "data_dir", cfg.DataDir)

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.SnapshotInterval = 20 * time.Second
	raftCfg.SnapshotThreshold = 64
	raftCfg.TrailingLogs = 128
	raftCfg.LogLevel = "INFO"

	logStore, err := boltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft", "logs.bolt"))
	if err != nil {
		return nil, err
	}
	stableStore, err := boltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft", "stable.bolt"))
	if err != nil {
		return nil, err
	}
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(cfg.DataDir, "snapshots"), 3, os.Stdout)
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stdout)
	if err != nil {
		return nil, err
	}

	f := &fsm{store: kv}
	r, err := raft.NewRaft(raftCfg, f, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return nil, err
	}
	if !hasState {
		logEvent("bootstrap_cluster", "node", cfg.NodeID, "peers", len(cfg.Peers))
		var servers []raft.Server
		for _, peer := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer.ID),
				Address: raft.ServerAddress(peer.RaftAddr),
			})
		}
		future := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := future.Error(); err != nil {
			return nil, err
		}
	} else {
		logEvent("recovery_existing_state", "node", cfg.NodeID)
	}

	peersByID := make(map[string]Peer, len(cfg.Peers))
	for _, peer := range cfg.Peers {
		peersByID[peer.ID] = peer
	}
	return &Store{raft: r, fsm: f, nodeID: cfg.NodeID, raftAddr: cfg.RaftAddr, grpcAddr: cfg.GRPCAddr, peersByID: peersByID, applyTimeout: cfg.ApplyTimeout}, nil
}

func (s *Store) Put(key string, value []byte) error {
	return s.apply(command{Op: "put", Key: key, Value: value})
}

func (s *Store) Delete(key string) error {
	return s.apply(command{Op: "delete", Key: key})
}

func (s *Store) Get(key string) ([]byte, error) {
	return s.fsm.store.Get(key)
}

func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *Store) LeaderGRPCAddr() string {
	leaderAddr, leaderID := s.raft.LeaderWithID()
	if leaderID != "" {
		if peer, ok := s.peersByID[string(leaderID)]; ok {
			return peer.GRPCAddr
		}
	}
	return string(leaderAddr)
}

func (s *Store) Status() Status {
	return Status{
		NodeID:       s.nodeID,
		GRPCAddr:     s.grpcAddr,
		RaftAddr:     s.raftAddr,
		State:        s.raft.State().String(),
		Leader:       s.LeaderGRPCAddr(),
		CommitIndex:  s.raft.CommitIndex(),
		LastIndex:    s.raft.LastIndex(),
		AppliedIndex: s.raft.AppliedIndex(),
		Peers:        s.Peers(),
	}
}

func (s *Store) Peers() []Peer {
	peers := make([]Peer, 0, len(s.peersByID))
	for _, peer := range s.peersByID {
		peers = append(peers, peer)
	}
	return peers
}

func (s *Store) Shutdown() error {
	return s.raft.Shutdown().Error()
}

func (s *Store) apply(cmd command) error {
	if !s.IsLeader() {
		logEvent("redirect_write", "node", s.nodeID, "op", cmd.Op, "key", cmd.Key, "leader", s.LeaderGRPCAddr())
		return ErrNotLeader{Leader: s.LeaderGRPCAddr()}
	}
	logEvent("raft_apply_start", "node", s.nodeID, "op", cmd.Op, "key", cmd.Key)
	payload, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	future := s.raft.Apply(payload, s.applyTimeout)
	if err := future.Error(); err != nil {
		logEvent("raft_apply_error", "node", s.nodeID, "op", cmd.Op, "key", cmd.Key, "error", err)
		return err
	}
	if err, ok := future.Response().(error); ok {
		logEvent("fsm_apply_error", "node", s.nodeID, "op", cmd.Op, "key", cmd.Key, "error", err)
		return err
	}
	logEvent("raft_commit", "node", s.nodeID, "op", cmd.Op, "key", cmd.Key, "commit_index", s.raft.CommitIndex(), "applied_index", s.raft.AppliedIndex())
	return nil
}

type Status struct {
	NodeID       string
	GRPCAddr     string
	RaftAddr     string
	State        string
	Leader       string
	CommitIndex  uint64
	LastIndex    uint64
	AppliedIndex uint64
	Peers        []Peer
}

type ErrNotLeader struct {
	Leader string
}

func (e ErrNotLeader) Error() string {
	if e.Leader == "" {
		return "not leader; leader unknown"
	}
	return fmt.Sprintf("not leader; leader is %s", e.Leader)
}

func IsNotLeader(err error) (ErrNotLeader, bool) {
	var target ErrNotLeader
	ok := errors.As(err, &target)
	return target, ok
}

type fsm struct {
	store *storage.Store
}

func (f *fsm) Apply(log *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}
	switch cmd.Op {
	case "put":
		logEvent("fsm_apply", "op", cmd.Op, "key", cmd.Key, "bytes", len(cmd.Value), "index", log.Index)
		return f.store.Put(cmd.Key, cmd.Value)
	case "delete":
		logEvent("fsm_apply", "op", cmd.Op, "key", cmd.Key, "index", log.Index)
		return f.store.Delete(cmd.Key)
	default:
		return fmt.Errorf("unknown command %q", cmd.Op)
	}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	items, err := f.store.Snapshot()
	if err != nil {
		return nil, err
	}
	data, err := storage.EncodeSnapshot(items)
	if err != nil {
		return nil, err
	}
	logEvent("snapshot_create", "keys", len(items), "bytes", len(data))
	return &snapshot{data: data}, nil
}

func (f *fsm) Restore(r io.ReadCloser) error {
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	items, err := storage.DecodeSnapshot(data)
	if err != nil {
		return err
	}
	logEvent("snapshot_restore", "keys", len(items), "bytes", len(data))
	return f.store.Restore(items)
}

type snapshot struct {
	data []byte
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	logEvent("snapshot_persist", "bytes", len(s.data))
	return sink.Close()
}

func (s *snapshot) Release() {}

func logEvent(event string, args ...any) {
	fields := []any{"event", event}
	fields = append(fields, args...)
	fmt.Println(formatLog(fields...))
}

func formatLog(args ...any) string {
	out := "level=info"
	for i := 0; i+1 < len(args); i += 2 {
		out += fmt.Sprintf(" %v=%s", args[i], logValue(args[i+1]))
	}
	return out
}

func logValue(value any) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("%q", v)
	case error:
		return fmt.Sprintf("%q", v.Error())
	default:
		return fmt.Sprintf("%v", v)
	}
}
