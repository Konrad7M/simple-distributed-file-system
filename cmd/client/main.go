package main

// TODO It should be possible use client to create datanodes and metadatanode on another nodes, with parameters like
// replication
// number of datanodes
// blocksize ???
//
// Tell which file should be uploaded to DFS

import (
	"aleksrosz/simple-distributed-file-system/common"
	"aleksrosz/simple-distributed-file-system/proto/file_request"
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Parse command-line flags
	serverIP := flag.String("s", "0.0.0.0", "Server IP address")
	port := flag.String("p", fmt.Sprint(common.DataNodeClientListenerPort), "Network port")
	filePath := flag.String("f", "default.txt", "Path of file to send")
	commandString := flag.String("c", "write", "command to be executed: write, read, delete")
	flag.Parse()
	m := make(map[string]int)
	m["write"] = 1
	m["read"] = 0
	m["delete"] = -1
	commandNumber := m[*commandString]

	// Check if required flags are set
	if *serverIP == "" || *filePath == "" {
		fmt.Println("Usage: go run client.go -s <server-ip> -f <file-path> [-p <port>]")
		return
	}

	array := strings.Split(*filePath, "/")
	fileName := array[len(array)-1]
	// Connect to the server
	nodeConnection, err := grpc.Dial(*serverIP+":"+*port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer nodeConnection.Close()
	fileRequestClient := file_request.NewHandleFileRequestsServiceClient(nodeConnection)
	if commandNumber == -1 {
		request := file_request.FileCommand{
			FileCommand: int32(commandNumber),
			FileName:    fileName,
			FileSize:    0,
			FileData:    nil,
		}
		fileRequestClient.SendFileRequest(context.Background(), &request)
		return
	}

	if commandNumber == 0 {
		request := file_request.FileCommand{
			FileCommand: int32(commandNumber),
			FileName:    fileName,
			FileSize:    0,
			FileData:    nil,
		}
		response, err := fileRequestClient.SendFileRequest(context.Background(), &request)
		if err != nil {
			println(err)
			return
		}
		file, err := create(fileName)
		file.Write(response.FileData)
		return
	}
	// Open the input file
	file, err := os.Open(*filePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	// Send the file data to the server
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}
	buf := make([]byte, fileInfo.Size())

	var bigBuff bytes.Buffer
	bigBuff.Read(buf)

	request := file_request.FileCommand{
		FileCommand: int32(commandNumber),
		FileName:    fileName,
		FileSize:    int32(fileInfo.Size()),
		FileData:    buf,
	}
	fileRequestClient.SendFileRequest(context.Background(), &request)
	fmt.Printf("File '%s' sent to server at %s:%s\n", *filePath, *serverIP, *port)
}
func create(p string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(p), 0770); err != nil {
		return nil, err
	}
	return os.Create(p)
}
