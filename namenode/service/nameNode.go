package service

import (
	"faydfs/config"
	"faydfs/proto"
	"faydfs/public"
	"strings"
	"sync"
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

type FileMeta struct {
	FileName      string
	FileSize      string
	ChildFileList []string
	IsDir         bool
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

var lock sync.RWMutex

type NameNode struct {
	// fileToBlock data needs to be persisted in disk
	// for recovery of namenode
	fileToBlock map[string][]blockMeta

	// blockToLocation is not necessary to be in disk
	// blockToLocation can be obtained from datanode blockreport()
	blockToLocation map[string][]replicaMeta

	// datanodeList contains list of datanode ipAddr
	datanodeList []DatanodeMeta

	fileList          map[string]FileMeta
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

// RenameFile 更改路径名称
func (nn *NameNode) RenameFile(src, des string) bool {
	lock.Lock()
	defer lock.Unlock()
	srcName, ok := nn.fileToBlock[src]
	if !ok {
		return false
	}
	nn.fileToBlock[des] = srcName
	nn.fileList[des] = nn.fileList[src]
	delete(nn.fileToBlock, src)
	delete(nn.fileList, src)
	return true
}

func (nn *NameNode) FileStat(path string) (*FileMeta, bool) {
	lock.RLock()
	defer lock.Unlock()
	meta, ok := nn.fileList[path]
	if !ok {
		return nil, false
	}
	return &meta, true
}

// MakeDir 最后一个/在客户端校验，保证不是以/为结尾
func (nn *NameNode) MakeDir(name string) (bool, error) {
	var path = name
	//校验路径是否存在
	for {
		if path == "/" {
			break
		}
		index := strings.LastIndex(path, "/")
		path = path[:index]
		if _, ok := nn.fileList[path]; !ok {
			return false, public.ErrPathNotFind
		}
	}
	lock.Lock()
	defer lock.Unlock()
	//判断目录是否已存在
	if _, ok := nn.fileList[name]; ok {
		return false, public.ErrDirAlreadyExists
	}
	nn.fileList[name] = FileMeta{IsDir: true}
	return true, nil
}

// DeletePath 删除指定路径的文件
func (nn *NameNode) DeletePath(name string) (bool, error) {
	var path = name
	//校验路径是否存在
	for {
		if path == "/" {
			break
		}
		index := strings.LastIndex(path, "/")
		path = path[:index]
		if _, ok := nn.fileList[path]; !ok {
			return false, public.ErrPathNotFind
		}
	}
	lock.Lock()
	defer lock.Unlock()
	// 判断是否为目录文件
	if meta, ok := nn.fileList[name]; !ok {
		return false, public.ErrPathNotFind
	} else if meta.IsDir { //存在且为目录文件
		//判断目录中是否有其他文件
		if len(meta.ChildFileList) > 0 {
			return false, public.ErrNotEmptyDir
		}
	}
	//路径指定为非目录
	delete(nn.fileToBlock, name)
	delete(nn.fileList, name)
	return true, nil
}

// GetDirMeta 获取目录元数据
func (nn *NameNode) GetDirMeta(name string) ([]*FileMeta, error) {
	var resultList []*FileMeta
	lock.Lock()
	defer lock.Unlock()

	if dir, ok := nn.fileList[name]; ok && dir.IsDir { // 如果路径存在且对应文件为目录
		for _, s := range dir.ChildFileList {
			fileMeta := nn.fileList[s]
			resultList = append(resultList, &fileMeta)
		}
		return resultList, nil
	} else if !ok { // 如果目录不存在
		return nil, public.ErrPathNotFind
	} else { // 非文件夹
		return nil, public.ErrNotDir
	}
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

func (nn *NameNode) GetBlockReport(bl *proto.BlockLocation) {
	blockName := bl.BlockName
	ipAddr := bl.IpAddr
	blockSize := bl.BlockSize
	replicaID := int(bl.ReplicaID)
	var state replicaState
	if bl.GetReplicaState() == proto.BlockLocation_ReplicaPending {
		state = ReplicaPending
	} else {
		state = ReplicaCommitted
	}

	blockMetaList, ok := nn.blockToLocation[blockName]
	if !ok {
		nn.blockToLocation[blockName] = []replicaMeta{{
			blockName: blockName,
			ipAddr:    ipAddr,
			fileSize:  blockSize,
			replicaID: replicaID,
			state:     state,
		}}
		return
	}
	for i, _ := range blockMetaList {
		if blockMetaList[i].ipAddr == ipAddr {
			blockMetaList[i].fileSize = blockSize
			blockMetaList[i].replicaID = replicaID
			blockMetaList[i].state = state

			return
		}
	}
	var meta = replicaMeta{
		blockName: blockName,
		ipAddr:    ipAddr,
		fileSize:  blockSize,
		replicaID: replicaID,
		state:     state,
	}
	blockMetaList = append(blockMetaList, meta)
	return
}
