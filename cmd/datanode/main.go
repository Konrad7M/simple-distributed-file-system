package main

import (
	"aleksrosz/simple-distributed-file-system/constants"
	"aleksrosz/simple-distributed-file-system/datanode"
	"fmt"
)

func main() {
	create, err := datanode.Create(datanode.Config{

		DataDir:       "./test_directory/dataNode01",
		Debug:         true,
		Port:          "8081",
		Addres:        "0.0.0.0",
		LeaderAddress: "0.0.0.0",
		LeaderPort:    "8080",
	})

	if err != nil {
		return
	}
	fmt.Println(create)

	go datanode.ListenHealthCheckServer("0.0.0.0:" + fmt.Sprint(constants.DataNodeHealthCheckListenerPort))
	go datanode.ListenFileRequestServiceServer("0.0.0.0:" + fmt.Sprint(constants.DataNodeClientListenerPort))
	datanode.SendBlockReport("0.0.0.0:" + fmt.Sprint(constants.MetaDataNodeBlockReportListeningPort))
}
