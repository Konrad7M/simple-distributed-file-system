package main

import (
	"aleksrosz/simple-distributed-file-system/common"
	"aleksrosz/simple-distributed-file-system/datanode"
	"fmt"
)

func main() {
	create, err := datanode.Create(datanode.Config{

		DataDir:       "./test_directory/dataNode01",
		Debug:         true,
		Port:          fmt.Sprint(common.DataNodeHealthCheckListenerPort),
		Addres:        "0.0.0.0",
		LeaderAddress: "0.0.0.0",
		LeaderPort:    fmt.Sprint(common.MetaDataNodeBlockReportListeningPort),
	})

	if err != nil {
		return
	}
	fmt.Println(create)

	go datanode.ListenHealthCheckServer("0.0.0.0:" + fmt.Sprint(common.DataNodeHealthCheckListenerPort))
	go datanode.ListenFileRequestServiceServer("0.0.0.0:" + fmt.Sprint(common.DataNodeClientListenerPort))
	datanode.SendBlockReport("0.0.0.0:" + fmt.Sprint(common.MetaDataNodeBlockReportListeningPort))
}
