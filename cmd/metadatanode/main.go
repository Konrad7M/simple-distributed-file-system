package main

import (
	"aleksrosz/simple-distributed-file-system/constants"
	"aleksrosz/simple-distributed-file-system/metadatanode"
	"fmt"
)

func main() {
	metadatanode1, err := metadatanode.Create(metadatanode.Config{
		DataDir: "./test_directory/metadatanode",
		Debug:   true,
		Port:    "8080",
		Addres:  "0.0.0.0",
	})

	if err != nil {
		return
	}
	fmt.Println(metadatanode1)

	go metadatanode.ListenBlockReportServiceServer("0.0.0.0:" + fmt.Sprint(constants.MetaDataNodeBlockReportListeningPort))

	//TODO Integration with DataNodes database to query nodes from database
	data, err := metadatanode.QueryHealthCheck("0.0.0.0:"+fmt.Sprint(constants.DataNodeHealthCheckListenerPort), 0)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(data)
}
