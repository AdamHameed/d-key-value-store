package server

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/adam/d-key-value-store/internal/raftstore"
	"github.com/adam/d-key-value-store/internal/server/kvpb"
	"github.com/adam/d-key-value-store/internal/storage"
)

func TestFollowerWriteRedirect(t *testing.T) {
	base := t.TempDir()
	peers := []raftstore.Peer{
		{ID: "node1", RaftAddr: freeAddr(t), GRPCAddr: "node1-grpc"},
		{ID: "node2", RaftAddr: freeAddr(t), GRPCAddr: "node2-grpc"},
		{ID: "node3", RaftAddr: freeAddr(t), GRPCAddr: "node3-grpc"},
	}

	type node struct {
		id    string
		kv    *storage.Store
		raft  *raftstore.Store
		close func()
	}
	var nodes []node
	defer func() {
		for _, n := range nodes {
			if n.raft != nil {
				_ = n.raft.Shutdown()
			}
			if n.kv != nil {
				_ = n.kv.Close()
			}
		}
	}()

	for _, peer := range peers {
		kv, err := storage.Open(filepath.Join(base, peer.ID, "badger"))
		if err != nil {
			t.Fatal(err)
		}
		rs, err := raftstore.Open(raftstore.Config{
			NodeID:       peer.ID,
			RaftAddr:     peer.RaftAddr,
			GRPCAddr:     peer.GRPCAddr,
			DataDir:      filepath.Join(base, peer.ID),
			Peers:        peers,
			ApplyTimeout: 2 * time.Second,
		}, kv)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, node{id: peer.ID, kv: kv, raft: rs})
	}

	var follower node
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if !n.raft.IsLeader() && n.raft.LeaderGRPCAddr() != "" {
				follower = n
				break
			}
		}
		if follower.raft != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if follower.raft == nil {
		t.Fatal("no follower observed")
	}

	resp, err := New(follower.id, follower.raft).Put(context.Background(), &kvpb.PutRequest{Key: "x", Value: []byte("y")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetOk() {
		t.Fatal("follower accepted write")
	}
	if resp.GetLeader() == "" {
		t.Fatal("follower did not return leader")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	return fmt.Sprintf("127.0.0.1:%d", lis.Addr().(*net.TCPAddr).Port)
}
