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
}
