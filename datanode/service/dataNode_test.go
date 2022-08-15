package service

import (
	"fmt"
	"log"
	"net"
	"os"
	"testing"
)

func TestRead(t *testing.T) {
	tempFile := "temp.txt"
	tempContent := "here is some temp content"
	createTempFile(tempFile, tempContent)
	b := GetBlock(tempFile, "r")
	temp := make([]byte, 0)
	for b.HasNextChunk() {
		chunk, size, _ := b.GetNextChunk()
		temp = append(temp, (*chunk)[:size]...)
	}
	if tempContent != string(temp) {
		t.Fail()
	}
	//deleteTempFile(tempFile)
	fmt.Println("read success")
}

func read() {
	tempFile := "temp.txt"
	tempContent := "here is some temp content"
	createTempFile(tempFile, tempContent)
	b := GetBlock(tempFile, "r")
	temp := make([]byte, 0)
	for b.HasNextChunk() {
		chunk, size, _ := b.GetNextChunk()
		temp = append(temp, (*chunk)[:size]...)
	}
	if tempContent != string(temp) {
		fmt.Println("read failed")
	}
	//deleteTempFile(tempFile)
	fmt.Println("read success")
	fileBytes := []byte(tempContent)
	iter := 0
	for iter < len(fileBytes) {
		end := iter + conf.IoSize
		if end > len(fileBytes) {
			end = len(fileBytes)
		}
		chunk := fileBytes[iter:end]
		fmt.Println("begin write")
		err := b.WriteChunk(chunk)
		fmt.Println("write done")
		if err != nil {
			fmt.Println(err)
		}
		iter = iter + conf.IoSize
	}
	for b.HasNextChunk() {
		chunk, size, _ := b.GetNextChunk()
		temp = append(temp, (*chunk)[:size]...)
	}
	fmt.Println(string(temp))
}

func TestReadTwice(t *testing.T) {
	//read()
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	fmt.Println(localAddr.IP)
}

func createTempFile(name string, content string) {
	file, err := os.Create(name)
	if err != nil {
		log.Fatal("cannot create file: ", err)
	}
	_, err1 := file.Write([]byte(content))
	if err1 != nil {
		log.Fatal("cannot write file: ", err1)
	}
}

func deleteTempFile(name string) {
	err := os.Remove(name)
	if err != nil {
		log.Fatal("cannot delete file: "+name, err)
	}
}

func TestWrite(t *testing.T) {
	tempFile := "temrp.txt"
	tempContent := "here is some temp content"
	// createTempFile(tempFile, tempContent)
	b := GetBlock(tempFile, "w")

	fileBytes := []byte(tempContent)
	iter := 0
	for iter < len(fileBytes) {
		end := iter + conf.IoSize
		if end > len(fileBytes) {
			end = len(fileBytes)
		}
		chunk := fileBytes[iter:end]

		err := b.WriteChunk(chunk)
		if err != nil {
			fmt.Println(err)
		}
		iter = iter + conf.IoSize
	}

	b = GetBlock(tempFile, "r")
	temp := make([]byte, 0)
	for b.HasNextChunk() {
		chunk, size, _ := b.GetNextChunk()
		temp = append(temp, (*chunk)[:size]...)
	}
	if tempContent != string(temp) {
		t.Fail()
	}

	deleteTempFile(tempFile)
}
