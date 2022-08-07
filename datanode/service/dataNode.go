package service

import (
	"bufio"
	"faydfs/config"
	"os"
)

// Block struct provides api to read and write block
type Block struct {
	blockName string
	offset    int
	chunkSize int
	reader    *bufio.Reader
	buffer    *[]byte
	dataRead  int
	file      *os.File
	blockSize int64
}

var conf = config.GetConfig()
