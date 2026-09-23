package kadmin_test

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nalgnaohel/kage/api/kadmin"
	pb "github.com/nalgnaohel/kage/api/rpc/kadmin"
	"github.com/nalgnaohel/kage/raft"
)

var leaderAddrPattern = regexp.MustCompile(`at (\S+)$`)

type testBroker struct {
	id       int32
	host     string
	port     int32
	node     *raft.Node
	grpcAddr string
	raftAddr string
	client   pb.KafkaAdminClient
}

func startBroker(t *testing.T, id int32, raftPort int) *testBroker {
	t.Helper()

	raftAddr := fmt.Sprintf("127.0.0.1:%d", raftPort)
	node, err := raft.NewNode(raft.Config{
		BrokerID:      id,
		BindAddr:      raftAddr,
		AdvertiseAddr: raftAddr,
		DataDir:       t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewNode(%d): %v", id, err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	grpcServer := grpc.NewServer()
	pb.RegisterKafkaAdminServer(grpcServer, kadmin.NewServer(node, "test-cluster"))
	go grpcServer.Serve(ln)
	t.Cleanup(grpcServer.Stop)

	host, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split grpc addr: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse grpc port: %v", err)
	}

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return &testBroker{
		id:       id,
		host:     host,
		port:     int32(port),
		node:     node,
		grpcAddr: ln.Addr().String(),
		raftAddr: raftAddr,
		client:   pb.NewKafkaAdminClient(conn),
	}
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
	t.Fatalf("broker %d did not become leader in time", node.BrokerID())
}

func findLeaderBroker(t *testing.T, brokers []*testBroker) *testBroker {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, b := range brokers {
			if b.node.Raft().State() == hraft.Leader {
				return b
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no broker reports itself leader in time")
	return nil
}

func waitForBrokerCount(t *testing.T, brokers []*testBroker, want int) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for {
		allReady := true
		for _, b := range brokers {
			if len(b.node.FSM().State().Brokers) != want {
				allReady = false
			}
		}
		if allReady {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("brokers did not converge to %d known brokers in time", want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForTopic(t *testing.T, node *raft.Node, topic string) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := node.FSM().State().Topics[topic]; ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker %d never observed topic %q", node.BrokerID(), topic)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestCreateTopic_RedirectsToLeader_ThenSucceeds(t *testing.T) {
	b1 := startBroker(t, 1, 19501)
	b2 := startBroker(t, 2, 19502)
	b3 := startBroker(t, 3, 19503)
	brokers := []*testBroker{b1, b2, b3}

	if err := b1.node.Bootstrap(); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	waitForLeader(t, b1.node)

	registerCmd, err := raft.NewRegisterBrokerCommand(raft.RegisterBrokerCommand{
		BrokerID: b1.id, Host: b1.host, Port: b1.port, RaftAddr: b1.raftAddr,
	})
	if err != nil {
		t.Fatalf("NewRegisterBrokerCommand: %v", err)
	}
	if _, err := b1.node.Propose(registerCmd, 2*time.Second); err != nil {
		t.Fatalf("Propose(RegisterBroker 1): %v", err)
	}

	ctx := context.Background()
	for _, b := range []*testBroker{b2, b3} {
		resp, err := b1.client.JoinCluster(ctx, &pb.JoinClusterRequest{
			BrokerId: b.id, Host: b.host, Port: b.port, RaftAddr: b.raftAddr,
		})
		if err != nil {
			t.Fatalf("JoinCluster(%d): %v", b.id, err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("JoinCluster(%d) failed: %s", b.id, resp.GetMessage())
		}
	}
	waitForBrokerCount(t, brokers, 3)

	leader := findLeaderBroker(t, brokers)
	var nonLeader *testBroker
	for _, b := range brokers {
		if b.id != leader.id {
			nonLeader = b
			break
		}
	}

	createResp, err := nonLeader.client.CreateTopic(ctx, &pb.CreateTopicRequest{
		TopicName: "orders", NumPartitions: 3, ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("CreateTopic against non-leader broker %d: %v", nonLeader.id, err)
	}
	if createResp.GetSuccess() {
		t.Fatalf("CreateTopic against non-leader broker %d unexpectedly succeeded", nonLeader.id)
	}
	match := leaderAddrPattern.FindStringSubmatch(createResp.GetMessage())
	if match == nil {
		t.Fatalf("CreateTopic redirect message %q did not carry a usable leader address", createResp.GetMessage())
	}
	redirectAddr := match[1]

	var redirected *testBroker
	for _, b := range brokers {
		if b.grpcAddr == redirectAddr {
			redirected = b
		}
	}
	if redirected == nil {
		t.Fatalf("redirect address %q did not match any broker's grpc address", redirectAddr)
	}

	retryResp, err := redirected.client.CreateTopic(ctx, &pb.CreateTopicRequest{
		TopicName: "orders", NumPartitions: 3, ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("CreateTopic against redirected leader broker %d: %v", redirected.id, err)
	}
	if !retryResp.GetSuccess() {
		t.Fatalf("CreateTopic against redirected leader broker %d failed: %s", redirected.id, retryResp.GetMessage())
	}

	var describer *testBroker
	for _, b := range brokers {
		if b.id != nonLeader.id && b.id != redirected.id {
			describer = b
		}
	}
	if describer == nil {
		t.Fatalf("no third broker available to describe from")
	}
	waitForTopic(t, describer.node, "orders")

	describeResp, err := describer.client.DescribeTopic(ctx, &pb.DescribeTopicRequest{TopicName: "orders"})
	if err != nil {
		t.Fatalf("DescribeTopic from broker %d: %v", describer.id, err)
	}
	if describeResp.GetNumPartitions() != 3 || describeResp.GetReplicationFactor() != 2 {
		t.Fatalf("DescribeTopic topic config = (%d partitions, rf %d), want (3, 2)",
			describeResp.GetNumPartitions(), describeResp.GetReplicationFactor())
	}

	sorted := []int32{1, 2, 3}
	for p := int32(0); p < 3; p++ {
		info, ok := describeResp.GetPartitions()[p]
		if !ok {
			t.Fatalf("partition %d missing from DescribeTopic response", p)
		}
		wantReplicas := []int32{sorted[p%3], sorted[(p+1)%3]}
		if !reflect.DeepEqual(info.GetReplicas(), wantReplicas) {
			t.Errorf("partition %d replicas = %v, want %v", p, info.GetReplicas(), wantReplicas)
		}
		if info.GetLeader() != wantReplicas[0] {
			t.Errorf("partition %d leader = %d, want %d", p, info.GetLeader(), wantReplicas[0])
		}
		if !reflect.DeepEqual(info.GetIsr(), wantReplicas) {
			t.Errorf("partition %d isr = %v, want %v (isr == replicas in Phase 2)", p, info.GetIsr(), wantReplicas)
		}
	}
}
