package main

import (
	"time"

	"github.com/dhcmrlchtdj/raftoy/server"
)

func main() {
	cluster := []string{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}
	closeS1 := server.Start("127.0.0.1:8000", cluster)
	closeS2 := server.Start("127.0.0.1:8001", cluster)
	closeS3 := server.Start("127.0.0.1:8002", cluster)

	time.Sleep(5 * time.Minute)
	closeS1()
	closeS2()
	closeS3()
}
