package main

import (
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/nalgnaohel/kage/api/kadmin"
	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/storage"

	pb "github.com/nalgnaohel/kage/api/rpc/api/proto/kadmin"
)

func main() {
	dataDir := flag.String("data-dir", "data", "base directory for partition logs")
	grpcAddr := flag.String("grpc-addr", ":9093", "listen address for the kadmin gRPC server")
	host := flag.String("host", "localhost", "this broker's advertised host")
	port := flag.Int("port", 9093, "this broker's advertised port")
	brokerID := flag.Int("broker-id", 0, "this broker's ID")
	clusterID := flag.String("cluster-id", "kage-cluster", "cluster identifier")
	flag.Parse()

	reg := broker.NewRegistry(*dataDir, storage.DefaultLogConfig(), *clusterID, int32(*brokerID), *host, int32(*port))
	if err := reg.Startup(); err != nil {
		log.Fatalf("registry startup: %v", err)
	}

	ln, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKafkaAdminServer(grpcServer, kadmin.NewServer(reg))

	log.Printf("kadmin gRPC server listening on %s", *grpcAddr)
	if err := grpcServer.Serve(ln); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
