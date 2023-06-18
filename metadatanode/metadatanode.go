package metadatanode

import (
	"aleksrosz/simple-distributed-file-system/common"
	pb "aleksrosz/simple-distributed-file-system/proto/block_report"
	pb2 "aleksrosz/simple-distributed-file-system/proto/health_check"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var Debug bool //TODO debug

type MetadataNodeState struct {
	mutex             sync.Mutex
	NodeID            string
	Addr              string
	LeaderAddress     string //Currently there is only one leader (metadatanode)
	HeartbeatInterval time.Duration
}

type Server struct {
	pb.BlockReportServiceServer
}

var BlockReportDatabase = NewDatabase()
var DatanodeDatabase = NewDatanodeDatabase()

func ListenBlockReportServiceServer(adres string) {
	lis, err := net.Listen("tcp", adres)
	if err != nil {
		log.Fatalf("failed to listen: %v+common.GetTraceString()", err)
	}

	log.Printf("Listening on %s", adres)
	s := grpc.NewServer()
	pb.RegisterBlockReportServiceServer(s, &Server{})

	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v+common.GetTraceString()", err)
	}

}

func QueryHealthCheck(adres string, dataNodeNumber int32) (*pb2.HealthCheckResponse, error) {
	//Connect for health check
	conn, err := grpc.Dial(adres, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("TEST TEST failed to connect"+common.GetTraceString(), err)
	}
	defer conn.Close()
	c := pb2.NewHealthClient(conn)
	data, err := queryHealthCheck(c)
	if err != nil {
		log.Printf("Error: %v"+common.GetTraceString(), err)
	}
	log.Printf("Response from server: %v", data)
	fmt.Println(adres)
	if data == nil {
		data = &pb2.HealthCheckResponse{
			Status:         0,
			DataNodeNumber: dataNodeNumber,
			IpAddress:      adres,
		}
	}
	return data, err
}

// Create a new MetadataNode
func Create(conf Config) (*MetadataNodeState, error) {
	var dn MetadataNodeState

	dn.Addr = conf.Addres + ":" + "8080"
	dn.HeartbeatInterval = 5 // Time in sec

	return &dn, nil
}
