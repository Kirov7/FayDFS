package service

import (
	"faydfs/config"
	"time"
)

// Block uhb
type blockMeta struct {
	blockName string
	gs        int64
	blockID   int
}

// BlockMeta wewer 每个block在不同副本上的位置
type replicaMeta struct {
	blockName string
	fileSize  int64
	ipAddr    string
	state     replicaState
	replicaID int
}

// DatanodeMeta metadata of datanode
type DatanodeMeta struct {
	IPAddr             string
	DiskUsage          uint64
	heartbeatTimeStamp int64
	status             datanodeStatus
}

type datanodeStatus string
type replicaState string

// namenode constants
const (
	datanodeDown     = datanodeStatus("datanodeDown")
	datanodeUp       = datanodeStatus("datanodeUp")
	ReplicaPending   = replicaState("pending")
	ReplicaCommitted = replicaState("committed")
)

type NameNode struct {
	// fileToBlock data needs to be persisted in disk
	// for recovery of namenode
	fileToBlock map[string][]blockMeta

	// blockToLocation is not necessary to be in disk
	// blockToLocation can be obtained from datanode blockreport()
	blockToLocation map[string][]replicaMeta

	// datanodeList contains list of datanode ipAddr
	datanodeList []DatanodeMeta

	blockSize         int64
	replicationFactor int
}

func GetNewNameNode(blockSize int64, replicationFactor int) *NameNode {
	namenode := &NameNode{
		fileToBlock:       make(map[string][]blockMeta),
		blockToLocation:   make(map[string][]replicaMeta),
		blockSize:         blockSize,
		replicationFactor: replicationFactor,
	}
	go namenode.heartbeatMonitor()
	return namenode
}

// RegisterDataNode 注册新的dn
func (nn *NameNode) RegisterDataNode(datanodeIPAddr string, diskUsage uint64) {
	meta := DatanodeMeta{
		IPAddr:             datanodeIPAddr,
		DiskUsage:          diskUsage,
		heartbeatTimeStamp: time.Now().Unix(),
		status:             datanodeUp,
	}
	// meta.heartbeatTimeStamp = time.Now().Unix()
	nn.datanodeList = append(nn.datanodeList, meta)
}

// 定时检测dn的状态是否可用
func (nn *NameNode) heartbeatMonitor() {
	for {
		heartbeatTimeout := config.GetConfig().HeartbeatTimeout
		heartbeatTimeoutDuration := time.Second * time.Duration(heartbeatTimeout)
		time.Sleep(heartbeatTimeoutDuration)

		for id, datanode := range nn.datanodeList {
			if time.Since(time.Unix(datanode.heartbeatTimeStamp, 0)) > heartbeatTimeoutDuration {
				nn.datanodeList[id].status = datanodeDown
			}
		}
	}
}

func (nn *NameNode) Heartbeat(datanodeIPAddr string, diskUsage uint64) {
	for id, datanode := range nn.datanodeList {
		if datanode.IPAddr == datanodeIPAddr {
			nn.datanodeList[id].heartbeatTimeStamp = time.Now().Unix()
			nn.datanodeList[id].DiskUsage = diskUsage
		}
	}
}

func (nn *NameNode) GetBlockReport(ipAddr, blockName string, blockSize int64) {
	blockMetaList, ok := nn.blockToLocation[blockName]
	if !ok {
		nn.blockToLocation[blockName] = []replicaMeta{{
			blockName: blockName,
			ipAddr:    ipAddr,
			fileSize:  blockSize,
		}}
		return
	}
	for i, _ := range blockMetaList {
		if blockMetaList[i].ipAddr == ipAddr {
			blockMetaList[i].fileSize = blockSize
			return
		}
	}
	var meta = replicaMeta{
		blockName: blockName,
		ipAddr:    ipAddr,
		fileSize:  blockSize,
	}
	blockMetaList = append(blockMetaList, meta)
	return
}
