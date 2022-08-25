package main

import (
	fayClient "faydfs/client"
	"fmt"
)

func main() {
	fmt.Println("start")
	client := fayClient.GetClient()
	fmt.Println("getClient")

	result := client.Put("D://testGPU.py", "/testGPU.py")
	fmt.Println("Put D://testGPU.py to /testGPU.py ", result.ResultExtraMsg)

	result2 := client.Get("/testGPU.py", "D://testSuccess.py")
	fmt.Println("Get /testGPU.py to D://testSuccess.py ", result2.ResultExtraMsg)

	result2 = client.Mkdir("/mydir")
	fmt.Println("Mkdir /mydir ", result2.ResultExtraMsg)

	result2 = client.Put("D://testGPU.py", "/mydir/testGPU.py")
	fmt.Println("Put D://testGPU.py to /mydir/testGPU.py ", result2.ResultExtraMsg)

	result2 = client.Stat("/mydir")
	fmt.Println("GetStat /mydir", result2.ResultExtraMsg)
	fmt.Println(result2.Data)

	result2 = client.List("/mydir")
	fmt.Println("List /mydir ", result2.ResultExtraMsg)
	fmt.Println(result2.Data)

	result2 = client.Get("/mydir/testGPU.py", "D://testGPU.py")
	fmt.Println("Get /mydir/testGPU.py to D://te/testGPU.py ", result2.ResultExtraMsg)

	result2 = client.Rename("/mydir/testGPU.py", "/mydir/test.py")
	fmt.Println("Rename /mydir/testGPU.py to /mydir/test.py", result2.ResultExtraMsg)

	result2 = client.Get("/mydir/test.py", "D://testRename.py")
	fmt.Println("Get /mydir/test.py to D://te/testGet.py ", result2.ResultExtraMsg)

	result2 = client.List("/mydir")
	fmt.Println("List /mydir ", result2.ResultExtraMsg)
	fmt.Println(result2.Data)

	result2 = client.Delete("/mydir/test.py")
	fmt.Println("Delete /mydir/test.py ", result2.ResultExtraMsg)

	result2 = client.Stat("/mydir")
	fmt.Println("GetStat /mydir", result2.ResultExtraMsg)
	fmt.Println(result2.Data)

	result2 = client.List("/mydir")
	fmt.Println("GetStat /mydir", result2.ResultExtraMsg)
	fmt.Println(result2.Data)

}
