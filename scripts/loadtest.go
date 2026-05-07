package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/adam/d-key-value-store/internal/server/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:5001", "gRPC address")
	peersRaw := flag.String("peers", "node1=localhost:5001,node2=localhost:5002,node3=localhost:5003", "comma-separated id=grpcAddr leader rewrite map")
	n := flag.Int("n", 1000, "number of put/get pairs")
	flag.Parse()

	peers := parsePeers(*peersRaw)
	target, err := discoverLeader(*addr, peers)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := kvpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	start := time.Now()
	for i := 0; i < *n; i++ {
		key := "load-" + strconv.Itoa(i)
		value := []byte("value-" + strconv.Itoa(i))
		put, err := client.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
		if err != nil {
			log.Fatal(err)
		}
		if !put.GetOk() {
			log.Fatalf("put failed: leader=%s error=%s", put.GetLeader(), put.GetError())
		}
		get, err := client.Get(ctx, &kvpb.GetRequest{Key: key})
		if err != nil {
			log.Fatal(err)
		}
		if !get.GetFound() || string(get.GetValue()) != string(value) {
			log.Fatalf("bad get for %s", key)
		}
	}
	elapsed := time.Since(start)
	ops := float64(*n*2) / elapsed.Seconds()
	fmt.Printf("leader=%s completed %d put/get pairs in %s (%.1f ops/sec)\n", target, *n, elapsed.Round(time.Millisecond), ops)
}

func discoverLeader(addr string, peers map[string]string) (string, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status, err := kvpb.NewKVClient(conn).Status(ctx, &kvpb.StatusRequest{})
	if err != nil {
		return "", err
	}
	if status.GetState() == "Leader" {
		return addr, nil
	}
	if status.GetLeader() == "" {
		return "", fmt.Errorf("leader unknown")
	}
	return rewriteLeader(status.GetLeader(), peers), nil
}

func parsePeers(raw string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(raw, ",") {
		idAddr := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(idAddr) == 2 {
			out[idAddr[0]] = idAddr[1]
		}
	}
	return out
}

func rewriteLeader(leader string, peers map[string]string) string {
	for id, addr := range peers {
		if strings.HasPrefix(leader, id+":") || leader == id {
			return addr
		}
	}
	return leader
}
