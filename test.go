package main

import (
	fayClient "faydfs/client"
	"fmt"
	"log"
)

func main() {
	client := fayClient.GetClient()
	result := client.Put("D://test.php", "/test.php")
	if result.ResultCode != 200 {
		log.Fatal(result.Data)
		return
	}
	fmt.Println(result.ResultExtraMsg)

	result2 := client.Get("/test.php", "D://testSuccess.php")
	fmt.Println(result2.ResultExtraMsg)
	result2 = client.Mkdir("/mydir")
	fmt.Println(result2.ResultExtraMsg)
	result2 = client.Put("D://test.php", "/mydir/test.php")
	fmt.Println(result2.ResultExtraMsg)
	result2 = client.Stat("/mydir")
	fmt.Println(result2.ResultExtraMsg)
	fmt.Println(result2.Data)
	result2 = client.Get("/mydir/test.php", "D://netdisk/test.php")
	fmt.Println(result2.ResultExtraMsg)
	result2 = client.Rename("/mydir/test.php", "/mydir/test.py")
	fmt.Println(result2.ResultExtraMsg)
	result2 = client.Get("/mydir/test.py", "D://netdisk/test.py")
	fmt.Println(result2.ResultExtraMsg)
	result2 = client.List("/mydir")
	fmt.Println(result2.ResultExtraMsg)
	fmt.Println(result2.Data)

	result2 = client.Delete("/mydir/test.py")
	fmt.Println(result2.ResultExtraMsg)
	result2 = client.Stat("/mydir")
	fmt.Println(result2.ResultExtraMsg)
	fmt.Println(result2.Data)

	result2 = client.List("/mydir")
	fmt.Println(result2.ResultExtraMsg)
	fmt.Println(result2.Data)

}
