package config

import (
	"encoding/json"
	"faydfs/public"
	"fmt"
	"os"
)

// util for parsing configs json

// Config contains configuration
type Config struct {
	BlockSize           int64
	Replica             int
	IoSize              int
	NameNodePort        string
	DataNodePort        string
	EditLog             string
	DataDir             string
	NameNodeHost        string
	HeartbeatInterval   int
	HeartbeatTimeout    int
	BlockReportInterval int
}

// GetConfig parses config.json and returns
// Config struct
func GetConfig() Config {
	var config Config
	// TODO：请改成自己的config地址
	//file := "./config/config.json"
	file := "D:\\Documents\\go_code\\FayDFS\\config\\config.json"
	configFile, err := os.Open(file)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer configFile.Close()
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&config)
	public.CreateDir(config.DataDir)
	return config
}
