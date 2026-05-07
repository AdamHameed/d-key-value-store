package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/adam/d-key-value-store/internal/raftstore"
	"github.com/adam/d-key-value-store/internal/server"
	"github.com/adam/d-key-value-store/internal/server/kvpb"
	"github.com/adam/d-key-value-store/internal/storage"
	"google.golang.org/grpc"
)

func main() {
	nodeID := flag.String("node-id", env("NODE_ID", "node1"), "node id")
	grpcAddr := flag.String("grpc-addr", env("GRPC_ADDR", ":50051"), "gRPC listen address")
	raftAddr := flag.String("raft-addr", env("RAFT_ADDR", ":7000"), "Raft listen/advertise address")
	dataDir := flag.String("data-dir", env("DATA_DIR", "data"), "data directory")
	cluster := flag.String("cluster", env("CLUSTER", "node1=127.0.0.1:7001,127.0.0.1:5001;node2=127.0.0.1:7002,127.0.0.1:5002;node3=127.0.0.1:7003,127.0.0.1:5003"), "semicolon-separated id=raftAddr,grpcAddr peers")
	flag.Parse()

	peers, err := parseCluster(*cluster)
	if err != nil {
		log.Fatal(err)
	}
	kv, err := storage.Open(filepath.Join(*dataDir, *nodeID, "badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer kv.Close()

	rs, err := raftstore.Open(raftstore.Config{
		NodeID:   *nodeID,
		RaftAddr: *raftAddr,
		GRPCAddr: *grpcAddr,
		DataDir:  filepath.Join(*dataDir, *nodeID),
		Peers:    peers,
	}, kv)
	if err != nil {
		log.Fatal(err)
	}
	defer rs.Shutdown()

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	kvpb.RegisterKVServer(grpcServer, server.New(*nodeID, rs))

	errs := make(chan error, 1)
	go func() {
		log.Printf("%s serving grpc=%s raft=%s", *nodeID, *grpcAddr, *raftAddr)
		errs <- grpcServer.Serve(lis)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-stop:
		log.Printf("received %s, shutting down", sig)
		grpcServer.GracefulStop()
	case err := <-errs:
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func parseCluster(raw string) ([]raftstore.Peer, error) {
	parts := strings.Split(raw, ";")
	peers := make([]raftstore.Peer, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idAndAddrs := strings.SplitN(part, "=", 2)
		if len(idAndAddrs) != 2 {
			return nil, fmt.Errorf("invalid peer %q", part)
		}
		addrs := strings.SplitN(idAndAddrs[1], ",", 2)
		if len(addrs) != 2 {
			return nil, fmt.Errorf("invalid peer addresses %q", part)
		}
		peers = append(peers, raftstore.Peer{ID: idAndAddrs[0], RaftAddr: addrs[0], GRPCAddr: addrs[1]})
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("cluster must contain at least one peer")
	}
	return peers, nil
}
