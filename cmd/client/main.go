package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adam/d-key-value-store/internal/server/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultPeers = "node1=localhost:5001,node2=localhost:5002,node3=localhost:5003"

type app struct {
	addr    string
	peers   map[string]string
	timeout time.Duration
}

func main() {
	log.SetFlags(0)

	global := flag.NewFlagSet("kvctl", flag.ExitOnError)
	addr := global.String("addr", "localhost:5001", "gRPC address")
	peersRaw := global.String("peers", defaultPeers, "comma-separated id=grpcAddr leader rewrite map")
	timeout := global.Duration("timeout", 5*time.Second, "request timeout")
	_ = global.Parse(os.Args[1:])

	if global.NArg() < 1 {
		usage()
		os.Exit(2)
	}

	a := app{addr: *addr, peers: parsePeers(*peersRaw), timeout: *timeout}
	cmd := global.Arg(0)
	args := global.Args()[1:]

	var err error
	switch cmd {
	case "put":
		err = a.put(args)
	case "get":
		err = a.get(args)
	case "delete":
		err = a.delete(args)
	case "status":
		err = a.status()
	case "leader":
		err = a.leader()
	case "load-test":
		err = a.loadTest(args)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func (a app) put(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: kvctl put <key> <value>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()
	return a.withLeader(ctx, func(client kvpb.KVClient) (string, bool, error) {
		resp, err := client.Put(ctx, &kvpb.PutRequest{Key: args[0], Value: []byte(args[1])})
		if err != nil {
			return "", false, err
		}
		if !resp.GetOk() {
			return resp.GetLeader(), false, nil
		}
		fmt.Printf("ok key=%s leader=%s\n", args[0], resp.GetLeader())
		return "", true, nil
	})
}

func (a app) get(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: kvctl get <key>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()
	return a.withLeader(ctx, func(client kvpb.KVClient) (string, bool, error) {
		resp, err := client.Get(ctx, &kvpb.GetRequest{Key: args[0]})
		if err != nil {
			return "", false, err
		}
		if resp.GetError() != "" {
			return resp.GetLeader(), false, nil
		}
		if !resp.GetFound() {
			fmt.Println("(not found)")
			return "", true, nil
		}
		fmt.Println(string(resp.GetValue()))
		return "", true, nil
	})
}

func (a app) delete(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: kvctl delete <key>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()
	return a.withLeader(ctx, func(client kvpb.KVClient) (string, bool, error) {
		resp, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: args[0]})
		if err != nil {
			return "", false, err
		}
		if !resp.GetOk() {
			return resp.GetLeader(), false, nil
		}
		fmt.Printf("ok key=%s leader=%s\n", args[0], resp.GetLeader())
		return "", true, nil
	})
}

func (a app) status() error {
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()

	addrs := sortedPeerAddrs(a.peers)
	if len(addrs) == 0 {
		addrs = []string{a.addr}
	}

	fmt.Printf("%-7s %-16s %-10s %-16s %-8s %-8s\n", "NODE", "ADDRESS", "STATE", "LEADER", "COMMIT", "APPLIED")
	for _, addr := range addrs {
		resp, err := statusAt(ctx, addr)
		if err != nil {
			fmt.Printf("%-7s %-16s %-10s %-16s %-8s %-8s\n", "?", addr, "down", "-", "-", "-")
			continue
		}
		fmt.Printf("%-7s %-16s %-10s %-16s %-8d %-8d\n",
			resp.GetNodeId(),
			addr,
			strings.ToLower(resp.GetState()),
			rewriteLeader(resp.GetLeader(), a.peers),
			resp.GetCommitIndex(),
			resp.GetAppliedIndex(),
		)
	}
	return nil
}

func (a app) leader() error {
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()
	leader, err := a.discoverLeader(ctx)
	if err != nil {
		return err
	}
	fmt.Println(leader)
	return nil
}

func (a app) loadTest(args []string) error {
	fs := flag.NewFlagSet("load-test", flag.ExitOnError)
	writes := fs.Int("writes", 1000, "number of writes")
	reads := fs.Int("reads", 1000, "number of reads")
	valueSize := fs.Int("value-size", 32, "bytes per value")
	concurrency := fs.Int("concurrency", 16, "concurrent client workers")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	leader, err := a.discoverLeader(ctx)
	if err != nil {
		return err
	}
	client, closeFn, err := connect(leader)
	if err != nil {
		return err
	}
	defer closeFn()

	value := []byte(strings.Repeat("x", *valueSize))
	start := time.Now()
	if err := runParallel(*concurrency, *writes, func(i int) error {
		key := fmt.Sprintf("bench-%06d", i)
		resp, err := client.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
		if err != nil {
			return err
		}
		if !resp.GetOk() {
			return fmt.Errorf("write redirected during load test: leader=%s error=%s", resp.GetLeader(), resp.GetError())
		}
		return nil
	}); err != nil {
		return err
	}
	if err := runParallel(*concurrency, *reads, func(i int) error {
		key := fmt.Sprintf("bench-%06d", i%max(1, *writes))
		resp, err := client.Get(ctx, &kvpb.GetRequest{Key: key})
		if err != nil {
			return err
		}
		if resp.GetError() != "" {
			return fmt.Errorf("read redirected during load test: leader=%s error=%s", resp.GetLeader(), resp.GetError())
		}
		return nil
	}); err != nil {
		return err
	}
	elapsed := time.Since(start)
	total := *writes + *reads
	fmt.Printf("leader=%s writes=%d reads=%d concurrency=%d elapsed=%s ops_sec=%.1f\n", leader, *writes, *reads, *concurrency, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
	return nil
}

func runParallel(concurrency, n int, fn func(int) error) error {
	if concurrency < 1 {
		concurrency = 1
	}
	var next atomic.Int64
	var stop atomic.Bool
	var once sync.Once
	var firstErr error
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if stop.Load() {
					return
				}
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				if err := fn(i); err != nil {
					once.Do(func() {
						firstErr = err
						stop.Store(true)
					})
					return
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func (a app) withLeader(ctx context.Context, fn func(kvpb.KVClient) (leader string, done bool, err error)) error {
	addr := a.addr
	for attempt := 0; attempt < 4; attempt++ {
		client, closeFn, err := connect(addr)
		if err != nil {
			return err
		}
		leader, done, err := fn(client)
		closeFn()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if leader == "" {
			return errors.New("node is not leader and did not report a leader")
		}
		addr = rewriteLeader(leader, a.peers)
	}
	return errors.New("too many leader redirects")
}

func (a app) discoverLeader(ctx context.Context) (string, error) {
	addrs := append([]string{a.addr}, sortedPeerAddrs(a.peers)...)
	seen := map[string]bool{}
	for _, addr := range addrs {
		if seen[addr] {
			continue
		}
		seen[addr] = true
		resp, err := statusAt(ctx, addr)
		if err != nil {
			continue
		}
		if resp.GetState() == "Leader" {
			return addr, nil
		}
		if resp.GetLeader() != "" {
			return rewriteLeader(resp.GetLeader(), a.peers), nil
		}
	}
	return "", errors.New("leader unknown")
}

func statusAt(ctx context.Context, addr string) (*kvpb.StatusResponse, error) {
	client, closeFn, err := connect(addr)
	if err != nil {
		return nil, err
	}
	defer closeFn()
	return client.Status(ctx, &kvpb.StatusRequest{})
}

func connect(addr string) (kvpb.KVClient, func(), error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, func() {}, err
	}
	return kvpb.NewKVClient(conn), func() { _ = conn.Close() }, nil
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

func sortedPeerAddrs(peers map[string]string) []string {
	ids := make([]string, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	addrs := make([]string, 0, len(ids))
	for _, id := range ids {
		addrs = append(addrs, peers[id])
	}
	return addrs
}

func rewriteLeader(leader string, peers map[string]string) string {
	for id, addr := range peers {
		if strings.HasPrefix(leader, id+":") || leader == id {
			return addr
		}
		if samePort(leader, addr) {
			return addr
		}
	}
	return leader
}

func samePort(left, right string) bool {
	leftParts := strings.Split(left, ":")
	rightParts := strings.Split(right, ":")
	if len(leftParts) == 0 || len(rightParts) == 0 {
		return false
	}
	return leftParts[len(leftParts)-1] == rightParts[len(rightParts)-1]
}

func usage() {
	fmt.Println(`usage:
  kvctl [--addr localhost:5001] put <key> <value>
  kvctl [--addr localhost:5001] get <key>
  kvctl [--addr localhost:5001] delete <key>
  kvctl status
  kvctl leader
  kvctl load-test --writes 1000 --reads 1000`)
}
