package public

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"faydfs/namenode/service"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

// CreateDir creates a dir
func CreateDir(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.Mkdir(path, 0640)
		if err != nil {
			fmt.Println(err, "err creating file ", path)
		}
	}
}

// Int2Bytes 整形转换成字节
func Int2Bytes(n int) []byte {
	x := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(bytesBuffer, binary.BigEndian, x)
	if err != nil {
		log.Fatal(err)
	}
	return bytesBuffer.Bytes()
}

// Bytes2Int 字节转换成整形
func Bytes2Int(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	err := binary.Read(bytesBuffer, binary.BigEndian, &x)
	if err != nil {
		log.Fatal(err)
	}
	return int(x)
}

func EncodeData(data interface{}) []byte {
	var encodeBuf bytes.Buffer
	gob.Register(map[string]*service.DatanodeMeta{})
	gob.Register(service.FileMeta{})
	enc := gob.NewEncoder(&encodeBuf)
	enc.Encode(data)
	return encodeBuf.Bytes()
}

func Decode2Entrys(in []byte) []service.DBEntry {
	dec := gob.NewDecoder(bytes.NewBuffer(in))
	req := []service.DBEntry{}
	dec.Decode(&req)
	return req
}
func MakeAnRandomElectionTimeout(base int) int {
	return RandIntRange(base, 2*base)
}

func RandIntRange(min int, max int) int {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return r1.Intn(max-min) + int(min)
}

func PrintDebugLog(msg string) {
	fmt.Printf("%s %s \n", time.Now().Format("2006-01-02 15:04:05"), msg)
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}
