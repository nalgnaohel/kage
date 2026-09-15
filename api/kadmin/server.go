package kadmin

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/nalgnaohel/kage/api/rpc/kadmin"
	"github.com/nalgnaohel/kage/broker"
)

type Server struct {
	pb.UnimplementedKafkaAdminServer
	registry *broker.Registry
}

func NewServer(registry *broker.Registry) *Server {
	return &Server{registry: registry}
}

func (s *Server) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if err := s.registry.CreateTopic(req.GetTopicName(), req.GetNumPartitions(), req.GetReplicationFactor()); err != nil {
		return &pb.CreateTopicResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.CreateTopicResponse{Success: true}, nil
}

func (s *Server) DeleteTopic(_ context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if err := s.registry.DeleteTopic(req.GetTopicName()); err != nil {
		return &pb.DeleteTopicResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.DeleteTopicResponse{Success: true}, nil
}

func (s *Server) ListTopics(_ context.Context, _ *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	return &pb.ListTopicsResponse{Topics: s.registry.ListTopics()}, nil
}

func (s *Server) DescribeTopic(_ context.Context, req *pb.DescribeTopicRequest) (*pb.DescribeTopicResponse, error) {
	cfg, partitionIDs, found := s.registry.DescribeTopic(req.GetTopicName())
	if !found {
		return nil, status.Errorf(codes.NotFound, "topic %s not found", req.GetTopicName())
	}

	// leader/replicas/isr are placeholders until Raft/ISR exist (Phase 2/3):
	// every partition's "replica set" is just this one broker.
	_, brokers := s.registry.GetClusterInfo()
	brokerID := brokers[0].BrokerID

	partitions := make(map[int32]*pb.PartitionInfo, len(partitionIDs))
	for _, p := range partitionIDs {
		partitions[p] = &pb.PartitionInfo{
			PartitionId: p,
			Leader:      brokerID,
			Replicas:    []int32{brokerID},
			Isr:         []int32{brokerID},
		}
	}

	return &pb.DescribeTopicResponse{
		TopicName:         cfg.Name,
		NumPartitions:     cfg.NumPartitions,
		ReplicationFactor: cfg.ReplicationFactor,
		Partitions:        partitions,
	}, nil
}

func (s *Server) GetClusterInfo(_ context.Context, _ *pb.GetClusterInfoRequest) (*pb.GetClusterInfoResponse, error) {
	clusterID, brokers := s.registry.GetClusterInfo()

	pbBrokers := make([]*pb.BrokerInfo, len(brokers))
	for i, b := range brokers {
		pbBrokers[i] = &pb.BrokerInfo{BrokerId: b.BrokerID, Host: b.Host, Port: b.Port}
	}

	return &pb.GetClusterInfoResponse{ClusterId: clusterID, Brokers: pbBrokers}, nil
}
