package main

import (
	"fmt"
	"time"

	"github.com/dhcmrlchtdj/raftoy/client"
	"github.com/dhcmrlchtdj/raftoy/server"
)

func main() {
	closeS0 := server.Start("127.0.0.1:8004", []string{"127.0.0.1:8004"})
	time.Sleep(2 * time.Second)
	closeS0()

	cluster := []string{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}
	closeS1 := server.Start("127.0.0.1:8000", cluster)
	closeS2 := server.Start("127.0.0.1:8001", cluster)
	closeS3 := server.Start("127.0.0.1:8002", cluster)

	time.Sleep(5 * time.Second)

	c := client.New(cluster)
	c.Conn()
	fmt.Printf("connect to cluster")
	c.Set("x", "1")
	val := c.Get("x")
	fmt.Printf("GET x=%v", *val)
	c.Del("x")
	val = c.Get("x")
	fmt.Printf("GET x=%v", val)

	time.Sleep(2 * time.Second)
	closeS1()

	time.Sleep(5 * time.Second)
	closeS2()
	closeS3()

}
