package main

import (
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/nalgnaohel/kage/api/kadmin"
	"github.com/nalgnaohel/kage/api/rawdata"
	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/storage"

	pb "github.com/nalgnaohel/kage/api/rpc/kadmin"
)

func main() {
	dataDir := flag.String("data-dir", "data", "base directory for partition logs")
	grpcAddr := flag.String("grpc-addr", ":9093", "listen address for the kadmin gRPC server")
	rawAddr := flag.String("raw-addr", ":9092", "listen address for the raw TCP data-plane server")
	host := flag.String("host", "localhost", "this broker's advertised host")
	port := flag.Int("port", 9093, "this broker's advertised port")
	brokerID := flag.Int("broker-id", 0, "this broker's ID")
	clusterID := flag.String("cluster-id", "kage-cluster", "cluster identifier")
	flag.Parse()

	reg := broker.NewRegistry(*dataDir, storage.DefaultLogConfig(), *clusterID, int32(*brokerID), *host, int32(*port))
	if err := reg.Startup(); err != nil {
		log.Fatalf("registry startup: %v", err)
	}

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
	pb.RegisterKafkaAdminServer(grpcServer, kadmin.NewServer(reg))

	log.Printf("kadmin gRPC server listening on %s", *grpcAddr)
	if err := grpcServer.Serve(ln); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
