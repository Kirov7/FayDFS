package service

import (
	"fmt"
	"log"
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
	deleteTempFile(tempFile)
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
