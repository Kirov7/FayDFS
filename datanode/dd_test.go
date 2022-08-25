package main

import (
	"faydfs/client"
	"fmt"
	"log"
	"testing"
)

func TestString(test *testing.T) {
	a := "localhost:8011"
	a = a[len(a)-1:]
	fmt.Println("localhost:5000" + a)
}

func TestDD(test *testing.T) {
	go PipelineServer("localhost:50001")
	go RunDataNode("localhost:8011")
	// 防止因为main中止造成协程中止
	defer func() {
		select {}
	}()
}

func TestDD2(test *testing.T) {
	go PipelineServer("localhost:50002")
	go RunDataNode("localhost:8012")
	// 防止因为main中止造成协程中止
	defer func() {
		select {}
	}()
}

func TestDD3(test *testing.T) {
	go PipelineServer("localhost:50003")
	go RunDataNode("localhost:8013")
	// 防止因为main中止造成协程中止
	defer func() {
		select {}
	}()
}

func TestClientPut(t *testing.T) {
	user := client.GetClient()
	result := user.Put("D:\\testGPU.py", "/test")
	if result.ResultCode != 200 {
		log.Fatal(result.Data)
		return
	}
	fmt.Println(result.ResultExtraMsg)
}
