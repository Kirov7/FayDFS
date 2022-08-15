package service

import (
	"bufio"
	"errors"
	"faydfs/config"
	"io"
	"log"
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

// 读取配置
// config路径问题，请在config/config.go文件中修改路径
var conf = config.GetConfig()

// ****************定义Block的结构函数****************

// initBlock 初始化chunk，blockName姑且认为是文件名，mode表示读写模式
func (b *Block) initBlock(blockName string, mode string) {
	var err error            // error变量
	var reader *bufio.Reader // io读
	var file *os.File        // 文件变量
	// 读模式初始化reader
	if mode == "r" {
		file, err = os.Open(blockName)
		reader = bufio.NewReader(file)
		// 写模式初始化
	} else if mode == "w" {
		file, err = os.OpenFile(blockName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	}
	if err != nil {
		log.Fatal("cannot open image file: ", err)
	}
	// 初始化结构
	b.file = file
	b.blockName = blockName
	b.reader = reader
	// 使用config文件设置的参数
	b.chunkSize = conf.IoSize
	b.blockSize = conf.BlockSize
	// 开辟一块chunkSize大小的缓冲区
	buffer := make([]byte, b.chunkSize)
	b.buffer = &buffer
	// dataRead = -1时表示没有到EOF，即可继续读
	b.dataRead = -1
	b.offset = 0
}

// GetBlock 获取新的Block
func GetBlock(blockName string, mode string) *Block {
	// 新建struct
	block := Block{}
	block.initBlock(blockName, mode)
	return &block
}

// HasNextChunk 检查是否可扩容
func (b *Block) HasNextChunk() bool {
	if b.dataRead != -1 {
		return true
	}
	// 从文件读取一块chunkSize大小的数据到block的缓冲区里
	n, err := b.reader.Read(*b.buffer)
	if err == io.EOF {
		b.dataRead = -1
		b.file.Close()
		return false
	}
	// 文件读取错误
	if err != nil {
		log.Fatal("cannot read chunk to buffer: ", err)
	}
	// dataRead设置为读取的字节数
	b.dataRead = n
	return true
}

// GetNextChunk 获取下一个Chunk的数据，返回数据和大小
// error 只有nil
func (b *Block) GetNextChunk() (*[]byte, int, error) {
	// 没有chunk了
	if b.dataRead == -1 {
		return nil, 0, nil
	}
	//
	n := b.dataRead
	b.dataRead = -1
	return b.buffer, n, nil
}

// WriteChunk 给出chunk，追加写入数据
func (b *Block) WriteChunk(chunk []byte) error {
	// 返回文件状态结构变量
	info, err := b.file.Stat()
	if err != nil {
		log.Fatal("cannot open the file: ", err)
	}
	// 获取当前文件大小
	currentBlockSize := info.Size()
	if b.blockSize >= (int64(len(chunk)) + currentBlockSize) {
		_, err := b.file.Write(chunk)
		if err != nil {
			log.Fatal("cannot write to file: ", err)
		}
		return nil // 写入完毕，不报错
	}
	// 定义错误类型
	var ErrFileExceedsBlockSize = errors.New("file is greater than block size")
	return ErrFileExceedsBlockSize
}

// Close 关闭文件
func (b *Block) Close() error {
	return b.file.Close()
}

// GetFileSize 获取文件大小
func (b *Block) GetFileSize() int64 {
	info, err := b.file.Stat()
	if err != nil {
		log.Fatal("error in reading the size")
	}
	return info.Size()
}
