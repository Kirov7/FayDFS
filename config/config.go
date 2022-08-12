package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
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
	LeaseSoftLimit      int
	LeaseHardLimit      int
}

var Conf *Config

// GetConfig parses config.json and returns
// Config struct
func GetConfig() *Config {
	return Conf
}

func init() {

	var curPath string
	for {
		curPath, _ = os.Getwd()
		curPathList := strings.Split(curPath, "\\")
		curPath = curPathList[len(curPathList)-1]
		if curPath == "FayDFS" {
			break
		}
		os.Chdir("../")
	}

	file := "./config/config.json"

	configFile, err := os.Open(file)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer configFile.Close()
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&Conf)
}
