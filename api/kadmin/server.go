package kadmin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/nalgnaohel/kage/api/rpc/kadmin"
	"github.com/nalgnaohel/kage/raft"
)

const proposeTimeout = 10 * time.Second

type Server struct {
	pb.UnimplementedKafkaAdminServer
	node      *raft.Node
	clusterID string
}

func NewServer(node *raft.Node, clusterID string) *Server {
	return &Server{node: node, clusterID: clusterID}
}

func (s *Server) proposeErrMessage(err error) string {
	var notLeader *raft.ErrNotLeader
	if errors.As(err, &notLeader) {
		if b, ok := s.node.FSM().State().Brokers[notLeader.LeaderID]; ok {
			return fmt.Sprintf("not leader; leader is broker %d at %s:%d", notLeader.LeaderID, b.Host, b.Port)
		}
		return fmt.Sprintf("not leader; leader is broker %d", notLeader.LeaderID)
	}
	return err.Error()
}

func (s *Server) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	cmd, err := raft.NewCreateTopicCommand(raft.CreateTopicCommand{
		Topic:             req.GetTopicName(),
		NumPartitions:     req.GetNumPartitions(),
		ReplicationFactor: req.GetReplicationFactor(),
	})
	if err != nil {
		return &pb.CreateTopicResponse{Success: false, Message: err.Error()}, nil
	}

	if _, err := s.node.Propose(cmd, proposeTimeout); err != nil {
		return &pb.CreateTopicResponse{Success: false, Message: s.proposeErrMessage(err)}, nil
	}
	return &pb.CreateTopicResponse{Success: true}, nil
}

func (s *Server) DeleteTopic(_ context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	cmd, err := raft.NewDeleteTopicCommand(raft.DeleteTopicCommand{Topic: req.GetTopicName()})
	if err != nil {
		return &pb.DeleteTopicResponse{Success: false, Message: err.Error()}, nil
	}

	if _, err := s.node.Propose(cmd, proposeTimeout); err != nil {
		return &pb.DeleteTopicResponse{Success: false, Message: s.proposeErrMessage(err)}, nil
	}
	return &pb.DeleteTopicResponse{Success: true}, nil
}

func (s *Server) ListTopics(_ context.Context, _ *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	state := s.node.FSM().State()

	topics := make([]string, 0, len(state.Topics))
	for name := range state.Topics {
		topics = append(topics, name)
	}
	sort.Strings(topics)

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

func (s *Server) DescribeTopic(_ context.Context, req *pb.DescribeTopicRequest) (*pb.DescribeTopicResponse, error) {
	state := s.node.FSM().State()

	meta, found := state.Topics[req.GetTopicName()]
	if !found {
		return nil, status.Errorf(codes.NotFound, "topic %s not found", req.GetTopicName())
	}

	partitions := make(map[int32]*pb.PartitionInfo, len(meta.Partitions))
	for p, assignment := range meta.Partitions {
		var leader int32
		if len(assignment.Replicas) > 0 {
			leader = assignment.Replicas[0]
		}
		partitions[p] = &pb.PartitionInfo{
			PartitionId: p,
			Leader:      leader,
			Replicas:    assignment.Replicas,
			Isr:         assignment.Replicas,
		}
	}

	return &pb.DescribeTopicResponse{
		TopicName:         req.GetTopicName(),
		NumPartitions:     meta.NumPartitions,
		ReplicationFactor: meta.ReplicationFactor,
		Partitions:        partitions,
	}, nil
}

func (s *Server) GetClusterInfo(_ context.Context, _ *pb.GetClusterInfoRequest) (*pb.GetClusterInfoResponse, error) {
	state := s.node.FSM().State()

	brokers := make([]*pb.BrokerInfo, 0, len(state.Brokers))
	for _, b := range state.Brokers {
		brokers = append(brokers, &pb.BrokerInfo{BrokerId: b.BrokerID, Host: b.Host, Port: b.Port})
	}
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].BrokerId < brokers[j].BrokerId })

	return &pb.GetClusterInfoResponse{ClusterId: s.clusterID, Brokers: brokers}, nil
}

func (s *Server) JoinCluster(_ context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	err := s.node.Join(raft.JoinRequest{
		BrokerID: req.GetBrokerId(),
		Host:     req.GetHost(),
		Port:     req.GetPort(),
		RaftAddr: req.GetRaftAddr(),
		RawAddr:  req.GetRawAddr(),
	})
	if err != nil {
		return &pb.JoinClusterResponse{Success: false, Message: s.proposeErrMessage(err)}, nil
	}
	return &pb.JoinClusterResponse{Success: true}, nil
}
