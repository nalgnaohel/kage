package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"regexp"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nalgnaohel/kage/api/kadmin"
	"github.com/nalgnaohel/kage/api/rawdata"
	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/raft"
	"github.com/nalgnaohel/kage/storage"

	pb "github.com/nalgnaohel/kage/api/rpc/kadmin"
)

const (
	leaderWaitTimeout = 15 * time.Second
	proposeTimeout    = 10 * time.Second
	joinDialTimeout   = 5 * time.Second
	joinMaxAttempts   = 5
	joinRetryDelay    = 500 * time.Millisecond
)

// Matches the trailing "at <addr>" in the redirect message
// api/kadmin.Server.proposeErrMessage produces for a not-leader response.
var leaderAddrPattern = regexp.MustCompile(`at (\S+)$`)

func main() {
	dataDir := flag.String("data-dir", "data", "base directory for partition logs")
	grpcAddr := flag.String("grpc-addr", ":9093", "listen address for the kadmin gRPC server")
	rawAddr := flag.String("raw-addr", ":9092", "listen address for the raw TCP data-plane server")
	host := flag.String("host", "localhost", "this broker's advertised host")
	port := flag.Int("port", 9093, "this broker's advertised port")
	brokerID := flag.Int("broker-id", 0, "this broker's ID")
	clusterID := flag.String("cluster-id", "kage-cluster", "cluster identifier")
	raftAddr := flag.String("raft-addr", ":9094", "bind address for the raft transport")
	raftPort := flag.Int("raft-port", 9094, "this broker's advertised raft port")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap a new single-node raft cluster")
	joinAddr := flag.String("join-addr", "", "an existing broker's kadmin gRPC address to join the raft cluster through")
	flag.Parse()

	if *bootstrap && *joinAddr != "" {
		log.Fatalf("raft: -bootstrap and -join-addr are mutually exclusive")
	}

	advertisedRaftAddr := fmt.Sprintf("%s:%d", *host, *raftPort)

	reg := broker.NewRegistry(*dataDir, storage.DefaultLogConfig())
	if err := reg.Startup(); err != nil {
		log.Fatalf("registry startup: %v", err)
	}

	node, err := raft.NewNode(raft.Config{
		BrokerID:      int32(*brokerID),
		BindAddr:      *raftAddr,
		AdvertiseAddr: advertisedRaftAddr,
		DataDir:       *dataDir,
	})
	if err != nil {
		log.Fatalf("raft: new node: %v", err)
	}

	selfInfo := raft.RegisterBrokerCommand{
		BrokerID: int32(*brokerID),
		Host:     *host,
		Port:     int32(*port),
		RaftAddr: advertisedRaftAddr,
	}

	if *bootstrap {
		if err := node.Bootstrap(); err != nil {
			log.Fatalf("raft: bootstrap: %v", err)
		}
		go registerSelf(node, selfInfo)
	} else if *joinAddr != "" {
		if err := joinCluster(*joinAddr, raft.JoinRequest(selfInfo)); err != nil {
			log.Fatalf("raft: join cluster: %v", err)
		}
	}

	reconciler := raft.NewReconciler(node, reg)
	go reconciler.Run(context.Background())

	rawLn, err := net.Listen("tcp", *rawAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	rawSrv := rawdata.NewServer(reg)
	go func() {
		log.Printf("raw data-plane server listening on %s", *rawAddr)
		if err := rawSrv.Serve(rawLn); err != nil {
			log.Fatalf("raw serve: %v", err)
		}
	}()

	ln, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKafkaAdminServer(grpcServer, kadmin.NewServer(node, *clusterID))

	log.Printf("kadmin gRPC server listening on %s", *grpcAddr)
	if err := grpcServer.Serve(ln); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// joinCluster dials addr's kadmin gRPC server and calls JoinCluster,
// following "not leader" redirects (parsed out of the same free-text
// message CreateTopic/DeleteTopic already use) until it either succeeds or
// exhausts joinMaxAttempts.
func joinCluster(addr string, req raft.JoinRequest) error {
	for attempt := 1; attempt <= joinMaxAttempts; attempt++ {
		resp, err := callJoinCluster(addr, req)
		if err != nil {
			return fmt.Errorf("dial %s: %w", addr, err)
		}

		if resp.GetSuccess() {
			log.Printf("raft: joined cluster via %s", addr)
			return nil
		}

		log.Printf("raft: join attempt %d against %s failed: %s", attempt, addr, resp.GetMessage())

		match := leaderAddrPattern.FindStringSubmatch(resp.GetMessage())
		if match == nil {
			return fmt.Errorf("join cluster: %s", resp.GetMessage())
		}
		addr = match[1]
		time.Sleep(joinRetryDelay)
	}
	return fmt.Errorf("join cluster: exceeded %d attempts, last tried %s", joinMaxAttempts, addr)
}

func callJoinCluster(addr string, req raft.JoinRequest) (*pb.JoinClusterResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), joinDialTimeout)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewKafkaAdminClient(conn)
	return client.JoinCluster(ctx, &pb.JoinClusterRequest{
		BrokerId: req.BrokerID,
		Host:     req.Host,
		Port:     req.Port,
		RaftAddr: req.RaftAddr,
	})
}

func registerSelf(node *raft.Node, info raft.RegisterBrokerCommand) {
	if err := node.WaitForLeader(leaderWaitTimeout); err != nil {
		log.Printf("raft: skipping self-registration: %v", err)
		return
	}

	cmd, err := raft.NewRegisterBrokerCommand(info)
	if err != nil {
		log.Printf("raft: self-registration failed: %v", err)
		return
	}

	if _, err := node.Propose(cmd, proposeTimeout); err != nil {
		log.Printf("raft: self-registration failed: %v", err)
		return
	}

	log.Printf("raft: registered broker %d (%s:%d, raft %s) in cluster metadata", info.BrokerID, info.Host, info.Port, info.RaftAddr)
}
