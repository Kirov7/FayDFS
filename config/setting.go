package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type Config struct {
	NameNode *NameNode `yaml:"NameNode"`
	DataNode *DataNode `yaml:"DataNode"`
	Block    *Block    `yaml:"Block"`
	Lease    *Lease    `yaml:"Lease"`
}

type NameNode struct {
	NameNodePort     string `yaml:"NameNodePort"`
	NameNodeHost     string `yaml:"NameNodeHost"`
	HeartbeatTimeout int    `yaml:"HeartbeatTimeout"`
}
type DataNode struct {
	HeartbeatInterval   int `yaml:"HeartbeatInterval"`
	BlockReportInterval int `yaml:"BlockReportInterval"`
	IoSize              int `yaml:"IoSize"`
}
type Block struct {
	BlockSize int64 `yaml:"BlockSize"`
	Replica   int   `yaml:"Replica"`
}
type Lease struct {
	LeaseSoftLimit int `yaml:"LeaseSoftLimit"`
	LeaseHardLimit int `yaml:"LeaseHardLimit"`
}

var conf *Config

func GetConfig() *Config {
	return conf
}

func init() {
	file, err := ioutil.ReadFile("config/config.yaml")
	if err != nil {
		log.Fatal("fail to read file:", err)
	}

	err = yaml.Unmarshal(file, &conf)
	if err != nil {
		log.Fatal("fail to yaml unmarshal:", err)
	}
	fmt.Println("encode success")

}
