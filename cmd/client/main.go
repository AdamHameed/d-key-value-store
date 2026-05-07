package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/adam/d-key-value-store/internal/server/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:5001", "gRPC address")
	peersRaw := flag.String("peers", "node1=localhost:5001,node2=localhost:5002,node3=localhost:5003", "comma-separated id=grpcAddr leader rewrite map")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	if flag.NArg() < 1 {
		usage()
		os.Exit(2)
	}
	peers := parsePeers(*peersRaw)
	cmd := flag.Arg(0)
	args := flag.Args()[1:]

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := run(ctx, *addr, peers, cmd, args); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, addr string, peers map[string]string, cmd string, args []string) error {
	for attempt := 0; attempt < 3; attempt++ {
		client, closeFn, err := connect(addr)
		if err != nil {
			return err
		}
		nextAddr, handled, err := call(ctx, client, cmd, args)
		closeFn()
		if err != nil {
			return err
		}
		if handled {
			return nil
		}
		if nextAddr == "" {
			return fmt.Errorf("node is not leader and did not report a leader")
		}
		addr = rewriteLeader(nextAddr, peers)
	}
	return fmt.Errorf("too many leader redirects")
}

func connect(addr string) (kvpb.KVClient, func(), error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, func() {}, err
	}
	return kvpb.NewKVClient(conn), func() { _ = conn.Close() }, nil
}

func call(ctx context.Context, client kvpb.KVClient, cmd string, args []string) (leader string, handled bool, err error) {
	switch cmd {
	case "put":
		if len(args) != 2 {
			return "", false, fmt.Errorf("usage: put <key> <value>")
		}
		resp, err := client.Put(ctx, &kvpb.PutRequest{Key: args[0], Value: []byte(args[1])})
		if err != nil {
			return "", false, err
		}
		if !resp.GetOk() {
			fmt.Printf("redirect leader=%s (%s)\n", resp.GetLeader(), resp.GetError())
			return resp.GetLeader(), false, nil
		}
		fmt.Printf("ok leader=%s\n", resp.GetLeader())
		return "", true, nil
	case "get":
		if len(args) != 1 {
			return "", false, fmt.Errorf("usage: get <key>")
		}
		resp, err := client.Get(ctx, &kvpb.GetRequest{Key: args[0]})
		if err != nil {
			return "", false, err
		}
		if resp.GetError() != "" {
			fmt.Printf("redirect leader=%s (%s)\n", resp.GetLeader(), resp.GetError())
			return resp.GetLeader(), false, nil
		}
		if !resp.GetFound() {
			fmt.Println("(not found)")
			return "", true, nil
		}
		fmt.Println(string(resp.GetValue()))
		return "", true, nil
	case "delete":
		if len(args) != 1 {
			return "", false, fmt.Errorf("usage: delete <key>")
		}
		resp, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: args[0]})
		if err != nil {
			return "", false, err
		}
		if !resp.GetOk() {
			fmt.Printf("redirect leader=%s (%s)\n", resp.GetLeader(), resp.GetError())
			return resp.GetLeader(), false, nil
		}
		fmt.Printf("ok leader=%s\n", resp.GetLeader())
		return "", true, nil
	case "status":
		resp, err := client.Status(ctx, &kvpb.StatusRequest{})
		if err != nil {
			return "", false, err
		}
		fmt.Printf("node=%s state=%s leader=%s last_index=%d applied_index=%d\n", resp.GetNodeId(), resp.GetState(), resp.GetLeader(), resp.GetLastIndex(), resp.GetAppliedIndex())
		for _, peer := range resp.GetPeers() {
			fmt.Printf("peer id=%s raft=%s grpc=%s\n", peer.GetId(), peer.GetRaftAddr(), peer.GetGrpcAddr())
		}
		return "", true, nil
	default:
		return "", false, fmt.Errorf("unknown command %q", cmd)
	}
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

func usage() {
	fmt.Println("usage: kv-client [--addr localhost:5001] <put|get|delete|status> ...")
}
