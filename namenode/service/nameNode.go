package service

import (
	"faydfs/config"
	"faydfs/proto"
	"time"
)

// Block uhb
type blockMeta struct {
	blockName string
	gs        int64
	blockID   int
}

// BlockMeta wewer
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

// RegisterDataNode adds ip to list of datanodeList
func (nn *NameNode) RegisterDataNode(datanodeIPAddr string, diskUsage uint64) {
	meta := DatanodeMeta{IPAddr: datanodeIPAddr, DiskUsage: diskUsage, heartbeatTimeStamp: time.Now().Unix(), status: datanodeUp}
	// meta.heartbeatTimeStamp = time.Now().Unix()
	nn.datanodeList = append(nn.datanodeList, meta)
}

// 定时检测dn的状态
func (nn *NameNode) heartbeatMonitor() {
	heartbeatTimeout := config.GetConfig().HeartbeatTimeout
	heartbeatTimeoutDuration := time.Second * time.Duration(heartbeatTimeout)
	time.Sleep(heartbeatTimeoutDuration)

	for id, datanode := range nn.datanodeList {
		if time.Since(time.Unix(datanode.heartbeatTimeStamp, 0)) > heartbeatTimeoutDuration {
			nn.datanodeList[id].DiskUsage = datanodeDown
		}
	}
	nn.heartbeatMonitor()
}

//读取文件的block块
func (nn *NameNode) GetLocation(name string) (*proto.FileLocationArr, error) {

	var blockReplicaLists []*proto.BlockReplicaList
	for i := 0; i < len(nn.fileToBlock[name]); i++ {
		//arr每行第一个，相当于原始元数据
		//先存第一个blockname
		var bname = nn.fileToBlock[name][i].blockName
		blockReplicaLists[i].BlockReplicaList[0].BlockName = bname
		blockReplicaLists[i].BlockReplicaList[0].IpAddr = nn.blockToLocation[bname][0].ipAddr
		blockReplicaLists[i].BlockReplicaList[0].BlockSize = nn.blockToLocation[bname][0].fileSize
		blockReplicaLists[i].BlockReplicaList[0].ReplicaID = int64(nn.blockToLocation[bname][0].replicaID)
		//不太理解state，下面这个设置注释掉了
		//blockReplicaLists[i].BlockReplicaList[0].ReplicaState = nn.blockToLocation[bname][i].state

		//之后每行后面的都是副本元数据
		for j := 1; j < len(nn.blockToLocation[bname]); j++ {
			var bname = nn.fileToBlock[name][i].blockName
			blockReplicaLists[i].BlockReplicaList[j].BlockName = bname
			blockReplicaLists[i].BlockReplicaList[j].IpAddr = nn.blockToLocation[bname][j].ipAddr
			blockReplicaLists[i].BlockReplicaList[j].BlockSize = nn.blockToLocation[bname][j].fileSize
			blockReplicaLists[i].BlockReplicaList[j].ReplicaID = int64(nn.blockToLocation[bname][j].replicaID)
			//blockReplicaLists[i].BlockReplicaList[j].ReplicaState = nn.blockToLocation[bname][j].state
		}

	}
	var arr = proto.FileLocationArr{FileBlocksList: blockReplicaLists}
	return &arr, nil
}

func (nn *NameNode) WriteLocation(name string, num int64) (*proto.FileLocationArr, error) {
	return nil, nil
}
