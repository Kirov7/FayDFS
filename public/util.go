package public

import (
	"fmt"
	"os"
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

// DiskUsage returns bytes of free space
//func DiskUsage(path string) uint64 {
//	di, err := disk.GetInfo(path)
//	if err != nil {
//		fmt.Println(err, "err")
//	}
//	return di.Ffree
//}
