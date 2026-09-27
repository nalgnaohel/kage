package replication_test

import (
	"encoding/json"
	"flag"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nalgnaohel/kage/api/rawdata"
	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/raft"
	"github.com/nalgnaohel/kage/storage"
)

var updateGolden = flag.Bool("update", false, "update .golden files with the current test output instead of comparing against them")

func assertGolden(t *testing.T, dir string, got any) {
	t.Helper()

	gotJSON, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatalf("marshal golden data: %v", err)
	}
	gotJSON = append(gotJSON, '\n')

	path := filepath.Join("testdata", dir, t.Name()+".golden")

	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatalf("create golden dir: %v", err)
		}
		if err := os.WriteFile(path, gotJSON, 0644); err != nil {
			t.Fatalf("write golden file: %v", err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden file %s (run with -update to create it): %v", path, err)
	}
	if string(gotJSON) != string(want) {
		t.Errorf("result does not match golden file %s\n--- got ---\n%s--- want ---\n%s", path, gotJSON, want)
	}
}

type goldenRecord struct {
	Offset uint64
	Value  string
}

func logRecords(t *testing.T, log *storage.Log, count int) []goldenRecord {
	t.Helper()

	recs := make([]goldenRecord, 0, count)
	for i := 0; i < count; i++ {
		v, err := log.Read(uint64(i))
		if err != nil {
			t.Fatalf("log.Read(%d): %v", i, err)
		}
		recs = append(recs, goldenRecord{Offset: uint64(i), Value: string(v)})
	}
	return recs
}

func newRegistry(t *testing.T) *broker.Registry {
	t.Helper()

	reg := broker.NewRegistry(t.TempDir(), storage.DefaultLogConfig())
	if err := reg.Startup(); err != nil {
		t.Fatalf("registry startup: %v", err)
	}
	return reg
}

func newRegistryLog(t *testing.T, topic string, partition int32) (*broker.Registry, *storage.Log) {
	t.Helper()

	reg := newRegistry(t)
	log, err := reg.CreateLog(topic, partition)
	if err != nil {
		t.Fatalf("CreateLog: %v", err)
	}
	return reg, log
}

// trackingListener remembers every net.Conn it hands out so a test can force
// an active connection closed (simulating a leader-side connection drop)
// without tearing down the listener itself.
type trackingListener struct {
	net.Listener

	mu    sync.Mutex
	conns []net.Conn
}

func (tl *trackingListener) Accept() (net.Conn, error) {
	conn, err := tl.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tl.mu.Lock()
	tl.conns = append(tl.conns, conn)
	tl.mu.Unlock()
	return conn, nil
}

func (tl *trackingListener) closeAll() {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	for _, c := range tl.conns {
		c.Close()
	}
}

// startRawServer starts a rawdata.Server backed by reg on a loopback port and
// returns its address plus a func that force-closes every connection
// currently accepted, letting a test simulate the leader side dropping a
// follower's connection.
func startRawServer(t *testing.T, reg *broker.Registry, onReplicaFetch func(topic string, partition int32, replicaID int32, fetchOffset uint64)) (addr string, drop func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	tl := &trackingListener{Listener: ln}

	srv := rawdata.NewServer(reg)
	srv.OnReplicaFetch = onReplicaFetch
	go srv.Serve(tl)

	return ln.Addr().String(), tl.closeAll
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func freePort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func newTestNode(t *testing.T, brokerID int32) *raft.Node {
	t.Helper()

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t)))
	node, err := raft.NewNode(raft.Config{
		BrokerID:      brokerID,
		BindAddr:      addr,
		AdvertiseAddr: addr,
		DataDir:       t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return node
}

func registerBroker(t *testing.T, node *raft.Node, brokerID int32) {
	t.Helper()

	cmd, err := raft.NewRegisterBrokerCommand(raft.RegisterBrokerCommand{
		BrokerID: brokerID,
		Host:     "localhost",
		Port:     9093,
		RaftAddr: net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t))),
		RawAddr:  net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t))),
	})
	if err != nil {
		t.Fatalf("NewRegisterBrokerCommand: %v", err)
	}
	if _, err := node.Propose(cmd, 2*time.Second); err != nil {
		t.Fatalf("Propose(RegisterBroker %d): %v", brokerID, err)
	}
}

func createTopic(t *testing.T, node *raft.Node, topic string, numPartitions, replicationFactor int32) {
	t.Helper()

	cmd, err := raft.NewCreateTopicCommand(raft.CreateTopicCommand{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil {
		t.Fatalf("NewCreateTopicCommand: %v", err)
	}
	if _, err := node.Propose(cmd, 2*time.Second); err != nil {
		t.Fatalf("Propose(CreateTopic %s): %v", topic, err)
	}
}
