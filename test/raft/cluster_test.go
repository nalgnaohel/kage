package raft_test

import (
	"reflect"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/nalgnaohel/kage/raft"
)

func TestCluster_ThreeNodes_ConvergeOnSameState(t *testing.T) {
	node1 := newTestNode(t, 1, "127.0.0.1:19301")
	node2 := newTestNode(t, 2, "127.0.0.1:19302")
	node3 := newTestNode(t, 3, "127.0.0.1:19303")
	nodes := []*raft.Node{node1, node2, node3}

	if err := node1.Bootstrap(); err != nil {
		t.Fatalf("Bootstrap() failed: %v", err)
	}
	waitForLeader(t, node1)

	registerSelfCmd, err := raft.NewRegisterBrokerCommand(raft.RegisterBrokerCommand{
		BrokerID: 1, Host: "localhost", Port: 9092, RaftAddr: "127.0.0.1:19301",
	})
	if err != nil {
		t.Fatalf("NewRegisterBrokerCommand() failed: %v", err)
	}
	if _, err := node1.Propose(registerSelfCmd, 2*time.Second); err != nil {
		t.Fatalf("Propose(RegisterBroker 1) failed: %v", err)
	}

	if err := node1.Join(raft.JoinRequest{BrokerID: 2, Host: "localhost", Port: 9093, RaftAddr: "127.0.0.1:19302"}); err != nil {
		t.Fatalf("Join(broker 2) failed: %v", err)
	}
	if err := node1.Join(raft.JoinRequest{BrokerID: 3, Host: "localhost", Port: 9094, RaftAddr: "127.0.0.1:19303"}); err != nil {
		t.Fatalf("Join(broker 3) failed: %v", err)
	}

	waitForConvergence(t, nodes, func(s raft.State) bool { return len(s.Brokers) == 3 })

	leader := findLeader(t, nodes)
	createCmd, err := raft.NewCreateTopicCommand(raft.CreateTopicCommand{
		Topic: "orders", NumPartitions: 3, ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("NewCreateTopicCommand() failed: %v", err)
	}
	if _, err := leader.Propose(createCmd, 2*time.Second); err != nil {
		t.Fatalf("Propose(CreateTopic) failed: %v", err)
	}

	deleteCmd, err := raft.NewDeleteTopicCommand(raft.DeleteTopicCommand{Topic: "orders"})
	if err != nil {
		t.Fatalf("NewDeleteTopicCommand() failed: %v", err)
	}
	createCmd2, err := raft.NewCreateTopicCommand(raft.CreateTopicCommand{
		Topic: "clicks", NumPartitions: 2, ReplicationFactor: 3,
	})
	if err != nil {
		t.Fatalf("NewCreateTopicCommand() failed: %v", err)
	}

	leader = findLeader(t, nodes)
	if _, err := leader.Propose(deleteCmd, 2*time.Second); err != nil {
		t.Fatalf("Propose(DeleteTopic) failed: %v", err)
	}
	leader = findLeader(t, nodes)
	if _, err := leader.Propose(createCmd2, 2*time.Second); err != nil {
		t.Fatalf("Propose(CreateTopic clicks) failed: %v", err)
	}

	waitForConvergence(t, nodes, func(s raft.State) bool {
		_, hasOrders := s.Topics["orders"]
		_, hasClicks := s.Topics["clicks"]
		return !hasOrders && hasClicks
	})

	want := node1.FSM().State()
	for _, n := range nodes {
		got := n.FSM().State()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("broker %d state diverged:\ngot:  %+v\nwant: %+v", n.BrokerID(), got, want)
		}
	}
	if want.Topics["clicks"].Partitions == nil || len(want.Topics["clicks"].Partitions) != 2 {
		t.Fatalf("clicks topic missing partition assignments: %+v", want.Topics["clicks"])
	}
}

func findLeader(t *testing.T, nodes []*raft.Node) *raft.Node {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.Raft().State() == hraft.Leader {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no node reports itself leader in time")
	return nil
}

func waitForConvergence(t *testing.T, nodes []*raft.Node, ready func(raft.State) bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for {
		allReady := true
		var states []raft.State
		for _, n := range nodes {
			s := n.FSM().State()
			states = append(states, s)
			if !ready(s) {
				allReady = false
			}
		}
		if allReady {
			for i := 1; i < len(states); i++ {
				if reflect.DeepEqual(states[0], states[i]) {
					continue
				}
				allReady = false
			}
		}
		if allReady {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cluster did not converge in time; states: %+v", states)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
