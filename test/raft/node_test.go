package raft_test

import (
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/nalgnaohel/kage/raft"
)

func newTestNode(t *testing.T, brokerID int32, addr string) *raft.Node {
	t.Helper()

	node, err := raft.NewNode(raft.Config{
		BrokerID:      brokerID,
		BindAddr:      addr,
		AdvertiseAddr: addr,
		DataDir:       t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}
	return node
}

func waitForLeader(t *testing.T, node *raft.Node) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if node.Raft().State() == hraft.Leader {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %d did not become leader in time", node.BrokerID())
}

func TestNode_Bootstrap_BecomesLeader(t *testing.T) {
	node := newTestNode(t, 1, "127.0.0.1:19201")

	if err := node.Bootstrap(); err != nil {
		t.Fatalf("Bootstrap() failed: %v", err)
	}
	waitForLeader(t, node)
}

func TestNode_Propose_CreateTopic_ReflectsInFSMState(t *testing.T) {
	node := newTestNode(t, 1, "127.0.0.1:19202")
	if err := node.Bootstrap(); err != nil {
		t.Fatalf("Bootstrap() failed: %v", err)
	}
	waitForLeader(t, node)

	cmd, err := raft.NewCreateTopicCommand(raft.CreateTopicCommand{
		Topic: "orders", NumPartitions: 2, ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("NewCreateTopicCommand() failed: %v", err)
	}

	if _, err := node.Propose(cmd, 2*time.Second); err != nil {
		t.Fatalf("Propose() failed: %v", err)
	}

	assertGolden(t, "node", node.FSM().State())
}

func TestNode_Propose_NotLeader_ReturnsErrNotLeader(t *testing.T) {
	node := newTestNode(t, 1, "127.0.0.1:19203")

	cmd, err := raft.NewCreateTopicCommand(raft.CreateTopicCommand{
		Topic: "orders", NumPartitions: 1, ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("NewCreateTopicCommand() failed: %v", err)
	}

	_, proposeErr := node.Propose(cmd, 2*time.Second)
	if proposeErr == nil {
		t.Fatalf("Propose() succeeded on a non-bootstrapped node, want ErrNotLeader")
	}
	if _, ok := proposeErr.(*raft.ErrNotLeader); !ok {
		t.Fatalf("Propose() error = %T, want *raft.ErrNotLeader", proposeErr)
	}
}
