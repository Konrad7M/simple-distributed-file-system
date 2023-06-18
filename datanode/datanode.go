package datanode

import (
	"aleksrosz/simple-distributed-file-system/proto/block_report"
	"aleksrosz/simple-distributed-file-system/proto/file_request"
	"bytes"
	"context"
	"errors"

	pb2 "aleksrosz/simple-distributed-file-system/proto/health_check"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var Debug bool //TODO debug
var listener net.Listener
var dataDir string

type DataNodeState struct {
	mutex             sync.Mutex
	NodeID            string
	heartbeatInterval time.Duration //TODO heartbeat
	Addr              string
	LeaderAddress     string
}

type healthCheckServer struct {
	pb2.HealthServer
}

type handleFileRequestServiceServer struct {
	file_request.HandleFileRequestsServiceServer
}

type FileCommand struct {
	fileCommand int32
	fileName    string
	fileSize    int
	fileData    bytes.Buffer
}

type FileResponse struct {
	message  string
	fileName string
	fileSize int
	fileData bytes.Buffer
}

func (s *handleFileRequestServiceServer) HandleFileService(ctx context.Context, in *file_request.FileCommand) (*file_request.FileResponse, error) {
	// 0 = odczyt  1 = zapis  -1 = usun
	switch in.FileCommand {
	case 0:
		{
			fileData, err := assembleFile(in.FileName)
			var retmessage string
			if fileData.Len() == 0 {
				retmessage = "no such file"
			} else {
				retmessage = "file retrieved"
			}

			return &file_request.FileResponse{
				Message:  retmessage,
				FileName: in.FileName,
				FileSize: int32(fileData.Len()),
				FileData: fileData.Bytes(),
			}, err
		}
	case 1:
		{
			err := splitFile(in.FileName, in.FileData, int(in.FileSize))
			return &file_request.FileResponse{
				Message:  "file saved",
				FileName: in.FileName,
				FileSize: 0,
			}, err
		}
	case -1:
		{
			deleteChunks(in.FileName)
			return &file_request.FileResponse{
				Message:  "file deleted",
				FileName: in.FileName,
				FileSize: 0,
			}, nil
		}

	}
	unknownCommandErr := errors.New("unknown command")
	return &file_request.FileResponse{}, unknownCommandErr
}

func ListenFileRequestServiceServer(adres string) {
	lis, err := net.Listen("tcp", adres)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()
	log.Printf("Listening on %s", adres)
	s := grpc.NewServer()
	file_request.RegisterHandleFileRequestsServiceServer(s, &handleFileRequestServiceServer{})
	// TODO RFC czemu to jest podkreślane

	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

func ListenHealthCheckServer(adres string) {
	lis, err := net.Listen("tcp", adres)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Printf("Listening on %s", adres)
	s := grpc.NewServer()
	//defer s.Stop()
	defer lis.Close()
	pb2.RegisterHealthServer(s, &healthCheckServer{})
	fmt.Println("test1")

	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	fmt.Println("test2")
}

func SendBlockReport(adres string) {
	for {
		//Connect for block report
		conn, err := grpc.Dial(adres, grpc.WithTransportCredentials(insecure.NewCredentials()))
		fmt.Println("test3")
		if err != nil {
			log.Fatal("failed to connect", err)
		}
		defer conn.Close()
		c := block_report.NewBlockReportServiceClient(conn)
		sendBlockReport(c)
		time.Sleep(5 * time.Second)
	}
}

// Create a new datanode
func Create(conf Config) (*DataNodeState, error) {
	var dn DataNodeState
	dn.Addr = conf.Addres + ":" + conf.Port
	dn.LeaderAddress = conf.LeaderAddress + ":" + conf.LeaderPort
	return &dn, nil
}

func assembleFile(fileName string) (bytes.Buffer, error) {
	fileData := bytes.Buffer{}

	chunkNum := 0
	chunkSize := 128
	for {
		chunkName := fmt.Sprintf("%s.%03d", fileName, chunkNum)
		path := filepath.Join(dataDir, chunkName)
		chunkFile, err := os.Open(path)
		if err != nil {
			break
		}
		buffer := make([]byte, chunkSize)
		chunkFile.Read(buffer)
		fileData.Write(buffer)
		chunkNum++
	}
	return fileData, nil
}

func splitFile(fileName string, fileData []byte, fileSize int) error {
	chunkNum := 0
	chunkSize := 128
	var chunkCount = fileSize / chunkSize
	var chunkPadding = chunkSize - (fileSize % chunkSize)
	fmt.Println("padding: ", chunkPadding)
	for {
		chunkName := fmt.Sprintf("%s.%03d", fileName, chunkNum)
		path := filepath.Join(dataDir, chunkName)
		chunkFile, err := os.Create(path)
		if err != nil {
			fmt.Println(err)
			return err
		}
		defer chunkFile.Close()
		_, err = chunkFile.Write(fileData[chunkNum*chunkSize : (chunkNum+1)*chunkSize])
		if err != nil {
			fmt.Println(err)
			return err
		}
		chunkNum++
		if chunkNum >= chunkCount {
			break
		}
	}
	return nil
}

func deleteChunks(fileName string) {
	fmt.Println("delete: ", fileName)
	chunkNum := 0
	for {
		chunkName := fmt.Sprintf("%s.%03d", fileName, chunkNum)
		path := filepath.Join(dataDir, chunkName)
		e := os.Remove(path)
		if e != nil {
			return
		}
	}
}

func create(p string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(p), 0770); err != nil {
		return nil, err
	}
	return os.Create(p)
}
