package public

import (
	"bytes"
	"encoding/binary"
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

func MakeAnRandomElectionTimeout(base int) int {
	return RandIntRange(base, 2*base)
}

func RandIntRange(min int, max int) int {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return r1.Intn(max-min) + int(min)
}
