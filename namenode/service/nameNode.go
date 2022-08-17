package service

import (
	"faydfs/config"
	"faydfs/proto"
	"faydfs/public"
	"fmt"
	"math/rand"
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
var (
	heartbeatTimeout = config.GetConfig().HeartbeatTimeout
	blockSize        = config.GetConfig().BlockSize
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

	fileList          map[string]*FileMeta
	blockSize         int64
	replicationFactor int
}

func GetNewNameNode(blockSize int64, replicationFactor int) *NameNode {
	namenode := &NameNode{
		fileToBlock:       make(map[string][]blockMeta),
		blockToLocation:   make(map[string][]replicaMeta),
		datanodeList:      []DatanodeMeta{},
		fileList:          make(map[string]*FileMeta),
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
	if src != "/" {
		index := strings.LastIndex(src, "/")
		parentPath := src[:index]
		for i, s := range nn.fileList[parentPath].ChildFileList {
			if s == src {
				nn.fileList[parentPath].ChildFileList[i] = des
			}
		}
	}
	return true
}

func (nn *NameNode) FileStat(path string) (*FileMeta, bool) {
	lock.RLock()
	defer lock.Unlock()
	meta, ok := nn.fileList[path]
	if !ok {
		return nil, false
	}
	return meta, true
}

// MakeDir 创建文件夹
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
	nn.fileList[name] = &FileMeta{IsDir: true}
	// 在父目录中追修改子文件
	if name != "/" {
		index := strings.LastIndex(path, "/")
		parentPath := name[:index]
		nn.fileList[parentPath].ChildFileList = append(nn.fileList[parentPath].ChildFileList, name)
	}
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
	// 在父目录中追修改子文件
	if name != "/" {
		index := strings.LastIndex(path, "/")
		parentPath := name[:index]
		nn.fileList[parentPath].ChildFileList = deleteChild(nn.fileList[parentPath].ChildFileList, name)
	}
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
			resultList = append(resultList, fileMeta)
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

func (nn *NameNode) PutSuccess(path string, arr *proto.FileLocationArr) {
	var blockList []blockMeta

	// 循环遍历每个block
	lock.Lock()
	defer lock.Unlock()
	for i, list := range arr.FileBlocksList {
		blockName := fmt.Sprintf(path, "_", i)
		bm := blockMeta{
			blockName: blockName,
			gs:        time.Now().Unix(),
			blockID:   i,
		}
		blockList = append(blockList, bm)

		var replicaList []replicaMeta
		// 循环遍历每个block存储的副本
		for j, list2 := range list.BlockReplicaList {
			var state replicaState
			if list2.GetReplicaState() == proto.BlockLocation_ReplicaPending {
				state = ReplicaPending
			} else {
				state = ReplicaCommitted
			}
			rm := replicaMeta{
				blockName: blockName,
				fileSize:  list2.BlockSize,
				ipAddr:    list2.IpAddr,
				state:     state,
				replicaID: j,
			}
			replicaList = append(replicaList, rm)
		}
		nn.blockToLocation[blockName] = replicaList
	}
	nn.fileToBlock[path] = blockList
	nn.fileList[path] = &FileMeta{
		FileName:      path,
		FileSize:      "",
		ChildFileList: nil,
		IsDir:         false,
	}
	// 在父目录中追加子文件
	if path != "/" {
		index := strings.LastIndex(path, "/")
		parentPath := path[:index]
		nn.fileList[parentPath].ChildFileList = append(nn.fileList[parentPath].ChildFileList, path)
	}
}

func (nn *NameNode) GetLocation(name string) (*proto.FileLocationArr, error) {
	panic("implement me")
}

func (nn *NameNode) WriteLocation(name string, num int64) (*proto.FileLocationArr, error) {
	var path = name
	//校验路径是否合法且存在
	for {
		if path == "/" {
			break
		}
		index := strings.LastIndex(path, "/")
		path = path[:index]
		if _, ok := nn.fileList[path]; !ok {
			return nil, public.ErrPathNotFind
		}
	}
	lock.Lock()
	defer lock.Unlock()
	//判断目标文件是否已存在
	if _, ok := nn.fileList[name]; ok {
		return nil, public.ErrDirAlreadyExists
	}
	fileArr := proto.FileLocationArr{}
	blocks := []*proto.BlockReplicaList{}

	// 一共需要num * replicationFactor个块 (最少切片块数 * 副本数)
	// 拥有的dn数量
	dnNum := len(nn.datanodeList)
	// 每个分片在随机存储在四个不通的可用服务器上
	for i := 0; i < int(num); i++ {
		replicaIndex, err := nn.selectDN(i, nn.replicationFactor, dnNum-1)
		if err != nil {
			return nil, err
		}
		replicaList := []*proto.BlockLocation{}
		// 每个block存在副本的位置信息
		for j, index := range replicaIndex {
			replicaList = append(replicaList, &proto.BlockLocation{
				IpAddr:       nn.datanodeList[index].IPAddr,
				BlockName:    fmt.Sprintf(name, "_", j),
				BlockSize:    blockSize,
				ReplicaID:    int64(j),
				ReplicaState: proto.BlockLocation_ReplicaPending,
			})
		}
		blocks = append(blocks, &proto.BlockReplicaList{
			BlockReplicaList: replicaList,
		})
	}
	fileArr.FileBlocksList = blocks
	return &fileArr, nil
}

// DeleteChild 删除指定元素
func deleteChild(a []string, elem string) []string {
	j := 0
	for _, v := range a {
		if v != elem {
			a[j] = v
			j++
		}
	}
	return a[:j]
}

// main: 主副本位置(不会被重复选择)
// needNum: 需要的副本数量
// section: 选择区间
func (nn *NameNode) selectDN(main, needNum, section int) ([]int, error) {

	//存放结果的slice
	nums := make([]int, 0)
	checkSet := make(map[int]interface{})
	failServer := make(map[int]interface{})
	checkSet[main] = nil
	failServer[main] = nil
	//随机数生成器，加入时间戳保证每次生成的随机数不一样
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(nums) < needNum {
		//生成随机数
		num := r.Intn(section)
		// 如果没有被选择过
		if _, ok := checkSet[num]; !ok {
			//且空间足够
			if nn.datanodeList[num].DiskUsage > uint64(blockSize) {
				nums = append(nums, num)
				checkSet[num] = nil
			}
		}
		failServer[num] = nil
		// 如果凑不齐需要的副本,则返回创建错误
		if len(failServer) >= section-needNum {
			return nil, public.ErrNotEnoughStorageSpace
		}
	}
	return nums, nil
}
