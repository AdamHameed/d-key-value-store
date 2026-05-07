package server

import (
	"context"
	"errors"

	"github.com/adam/d-key-value-store/internal/raftstore"
	"github.com/adam/d-key-value-store/internal/server/kvpb"
	"github.com/adam/d-key-value-store/internal/storage"
)

type KVServer struct {
	kvpb.UnimplementedKVServer
	nodeID string
	store  *raftstore.Store
}

func New(nodeID string, store *raftstore.Store) *KVServer {
	return &KVServer{nodeID: nodeID, store: store}
}

func (s *KVServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if err := s.store.Put(req.GetKey(), req.GetValue()); err != nil {
		if notLeader, ok := raftstore.IsNotLeader(err); ok {
			return &kvpb.PutResponse{Ok: false, Leader: notLeader.Leader, Error: err.Error()}, nil
		}
		return &kvpb.PutResponse{Ok: false, Leader: s.store.LeaderGRPCAddr(), Error: err.Error()}, nil
	}
	return &kvpb.PutResponse{Ok: true, Leader: s.store.LeaderGRPCAddr()}, nil
}

func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if !s.store.IsLeader() {
		leader := s.store.LeaderGRPCAddr()
		return &kvpb.GetResponse{Found: false, Leader: leader, Error: "not leader; reads are served by the leader"}, nil
	}
	value, err := s.store.Get(req.GetKey())
	if errors.Is(err, storage.ErrNotFound) {
		return &kvpb.GetResponse{Found: false, Leader: s.store.LeaderGRPCAddr()}, nil
	}
	if err != nil {
		return &kvpb.GetResponse{Found: false, Leader: s.store.LeaderGRPCAddr(), Error: err.Error()}, nil
	}
	return &kvpb.GetResponse{Found: true, Value: value, Leader: s.store.LeaderGRPCAddr()}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if err := s.store.Delete(req.GetKey()); err != nil {
		if notLeader, ok := raftstore.IsNotLeader(err); ok {
			return &kvpb.DeleteResponse{Ok: false, Leader: notLeader.Leader, Error: err.Error()}, nil
		}
		return &kvpb.DeleteResponse{Ok: false, Leader: s.store.LeaderGRPCAddr(), Error: err.Error()}, nil
	}
	return &kvpb.DeleteResponse{Ok: true, Leader: s.store.LeaderGRPCAddr()}, nil
}

func (s *KVServer) Status(ctx context.Context, req *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	status := s.store.Status()
	resp := &kvpb.StatusResponse{
		NodeId:       s.nodeID,
		State:        status.State,
		Leader:       status.Leader,
		LastIndex:    status.LastIndex,
		AppliedIndex: status.AppliedIndex,
	}
	for _, peer := range status.Peers {
		resp.Peers = append(resp.Peers, &kvpb.Peer{
			Id:       peer.ID,
			RaftAddr: peer.RaftAddr,
			GrpcAddr: peer.GRPCAddr,
		})
	}
	return resp, nil
}
