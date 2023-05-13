package datanode

import (
	pb "aleksrosz/simple-distributed-file-system/proto"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var Debug bool //TODO debug

type DataNodeState struct {
	mutex  sync.Mutex
	NodeID string
	//heartbeatInterval time.Duration //TODO heartbeat
	Addr          string
	LeaderAddress string
}

// Create a new datanode
func Create(conf Config) (*DataNodeState, error) {
	var dn DataNodeState

	dn.Addr = conf.Addres + ":" + conf.Port
	// TODO Networking
	//dn.heartbeatInterval = conf.HeartbeatInterval
	//dn.LeaderAddress = conf.LeaderAddress

	// TODO gRPC
	//lis, err := net.Listen("tcp", dn.Addr)
	//	if err != nil {
	//		log.Fatalf("failed to listen: %v", err)
	//	}

	//go dn.grpcstart(conf.Listener) // Start the RPC server https://grpc.io/
	//go dn.Heartbeat() // Check what is the best way to do this.

	conn, err := grpc.Dial(dn.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("filed to connect", err)
	}
	defer conn.Close()

	clientConnection, err := net.Dial("tcp", "8085")
	if err != nil {
		fmt.Println(err)
		return &dn, err
	}
	defer clientConnection.Close()

	go handleConnection(clientConnection)

	c := pb.NewBlockReportfServiceClient(conn)

	createBlockReport(c)

	return &dn, nil
}

func handleConnection(c net.Conn) {
	buf := make([]byte, 4096)

	for {
		n, err := c.Read(buf)
		if err != nil || n == 0 {
			break
		}
	}

	chunkSize := 100
	fileName := "czumuliugnma"

	// Create output files for each chunk
	chunkNum := 1
	for {
		chunkName := fmt.Sprintf("%s.%03d", fileName, chunkNum)
		chunkFile, err := os.Create(chunkName)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer chunkFile.Close()
		// Read input data and write to the output file
		buf := make([]byte, chunkSize)
		chunkFile.Write(buf[:n])
		// Increment the chunk number and continue until end of file
		chunkNum++
	}
}
