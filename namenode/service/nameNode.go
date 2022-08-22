package service

import (
	"faydfs/config"
	"faydfs/proto"
	"faydfs/public"
	"fmt"
	"log"
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

// FileMeta 文件元数据
type FileMeta struct {
	FileName      string
	FileSize      uint64
	ChildFileList map[string]uint64
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

func (nn *NameNode) ShowLog() {
	for i, node := range nn.datanodeList {
		log.Printf("No.%d  ", i)
		log.Printf("ip: %v", node.IPAddr)
		log.Printf("status: %v\n", node.status)
	}
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
	namenode.fileList["/"] = &FileMeta{FileName: "/", IsDir: true, ChildFileList: map[string]uint64{}}
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
func (nn *NameNode) RenameFile(src, des string) error {
	if src == "/" {
		return public.ErrCanNotChangeRootDir
	}
	lock.Lock()
	defer lock.Unlock()
	srcName, ok := nn.fileToBlock[src]
	if !ok {
		return public.ErrFileNotFound
	}
	nn.fileToBlock[des] = srcName
	nn.fileList[des] = nn.fileList[src]
	delete(nn.fileToBlock, src)
	delete(nn.fileList, src)
	if src != "/" {
		index := strings.LastIndex(src, "/")
		parentPath := src[:index]
		if parentPath == "" {
			parentPath = "/"
		}
		srcSize, _ := nn.fileList[parentPath].ChildFileList[src]
		delete(nn.fileList[parentPath].ChildFileList, src)
		nn.fileList[parentPath].ChildFileList[des] = srcSize
	}
	return nil
}

func (nn *NameNode) FileStat(path string) (*FileMeta, bool) {
	lock.RLock()
	defer lock.RUnlock()
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
		if path == "" {
			break
		}
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
	nn.fileList[name] = &FileMeta{IsDir: true, ChildFileList: map[string]uint64{}}
	// 在父目录中追修改子文件
	if name != "/" {
		index := strings.LastIndex(name, "/")
		parentPath := name[:index]
		if parentPath == "" {
			parentPath = "/"
		}
		nn.fileList[parentPath].ChildFileList[name] = 0
	}
	return true, nil
}

// DeletePath 删除指定路径的文件
func (nn *NameNode) DeletePath(name string) (bool, error) {
	if name == "/" {
		return false, public.ErrCanNotChangeRootDir
	}
	var path = name
	//校验路径是否存在
	for {
		if path == "/" {
			break
		}
		index := strings.LastIndex(path, "/")
		path = path[:index]
		if path == "" {
			path = "/"
		}
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
		if parentPath == "" {
			parentPath = "/"
		}
		fmt.Println(parentPath)
		// 删除父目录中记录的文件
		deleteSize, _ := nn.fileList[parentPath].ChildFileList[name]
		delete(nn.fileList[parentPath].ChildFileList, name)
		srcSize := nn.fileList[parentPath].FileSize
		// 更改父目录的大小
		nn.fileList[parentPath].FileSize = srcSize - deleteSize
	}
	return true, nil
}

// GetDirMeta 获取目录元数据
func (nn *NameNode) GetDirMeta(name string) ([]*FileMeta, error) {
	var resultList []*FileMeta
	lock.Lock()
	defer lock.Unlock()

	if dir, ok := nn.fileList[name]; ok && dir.IsDir { // 如果路径存在且对应文件为目录
		for k, _ := range dir.ChildFileList {
			fileMeta := nn.fileList[k]
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
			fmt.Println("update dn :", datanodeIPAddr, "diskUsage :", diskUsage)
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

func (nn *NameNode) PutSuccess(path string, fileSize uint64, arr *proto.FileLocationArr) {
	var blockList []blockMeta
	fmt.Println("putsuccess arr: ", arr)
	// 循环遍历每个block
	lock.Lock()
	defer lock.Unlock()
	for i, list := range arr.FileBlocksList {
		//blockName := fmt.Sprintf("%v%v%v", path, "_", i)
		bm := blockMeta{
			blockName: list.BlockReplicaList[i].BlockName,
			gs:        time.Now().Unix(),
			blockID:   i,
		}
		blockList = append(blockList, bm)
		fmt.Println("pustsuccess blockList: ", blockList)
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
				blockName: list.BlockReplicaList[i].BlockName,
				fileSize:  list2.BlockSize,
				ipAddr:    list2.IpAddr,
				state:     state,
				replicaID: j,
			}
			replicaList = append(replicaList, rm)
		}
		nn.blockToLocation[list.BlockReplicaList[i].BlockName] = replicaList
	}
	fmt.Println("putsuccess: ", blockList)
	nn.fileToBlock[path] = blockList
	nn.fileList[path] = &FileMeta{
		FileName:      path,
		FileSize:      fileSize,
		ChildFileList: nil,
		IsDir:         false,
	}
	// 在父目录中追加子文件
	if path != "/" {
		index := strings.LastIndex(path, "/")
		parentPath := path[:index]
		if parentPath == "" {
			parentPath = "/"
		}
		srcSize := nn.fileList[parentPath].FileSize
		// 更改父目录的大小
		//TODO 如果要修改的话需要逐层修改父级目录的大小
		nn.fileList[parentPath].FileSize = srcSize + fileSize
		nn.fileList[parentPath].ChildFileList[path] = fileSize

	}
}

func (nn *NameNode) GetLocation(name string) (*proto.FileLocationArr, error) {

	blockReplicaLists := []*proto.BlockReplicaList{}
	if block, ok := nn.fileToBlock[name]; !ok {
		return nil, public.ErrPathNotFind
	} else {
		for _, meta := range block {
			// 每个block存在副本的位置信息
			if replicaLocation, exit := nn.blockToLocation[meta.blockName]; !exit {
				return nil, public.ErrReplicaNotFound
			} else {
				replicaList := []*proto.BlockLocation{}
				for _, location := range replicaLocation {
					var state proto.BlockLocation_ReplicaMetaState
					if location.state == ReplicaCommitted {
						state = proto.BlockLocation_ReplicaCommitted
					} else {
						state = proto.BlockLocation_ReplicaPending
					}
					replicaList = append(replicaList, &proto.BlockLocation{
						IpAddr:       location.ipAddr,
						BlockName:    location.blockName,
						BlockSize:    location.fileSize,
						ReplicaID:    int64(location.replicaID),
						ReplicaState: state,
					})
				}
				blockReplicaLists = append(blockReplicaLists, &proto.BlockReplicaList{
					BlockReplicaList: replicaList,
				})
			}
		}
	}
	//
	//// 文件对应的块
	//for i := 0; i < len(nn.fileToBlock[name]); i++ {
	//	//arr每行第一个，相当于原始元数据
	//	//先存第一个blockname
	//
	//	var bname = nn.fileToBlock[name][i].blockName
	//	blockReplicaLists[i].BlockReplicaList[0].BlockName = bname
	//	blockReplicaLists[i].BlockReplicaList[0].IpAddr = nn.blockToLocation[bname][0].ipAddr
	//	blockReplicaLists[i].BlockReplicaList[0].BlockSize = nn.blockToLocation[bname][0].fileSize
	//	blockReplicaLists[i].BlockReplicaList[0].ReplicaID = int64(nn.blockToLocation[bname][0].replicaID)
	//	//不太理解state，下面这个设置注释掉了
	//	//blockReplicaLists[i].BlockReplicaList[0].ReplicaState = nn.blockToLocation[bname][i].state
	//
	//	//之后每行后面的都是副本元数据
	//	for j := 1; j < len(nn.blockToLocation[bname]); j++ {
	//		var bname = nn.fileToBlock[name][i].blockName
	//		blockReplicaLists[i].BlockReplicaList[j].BlockName = bname
	//		blockReplicaLists[i].BlockReplicaList[j].IpAddr = nn.blockToLocation[bname][j].ipAddr
	//		blockReplicaLists[i].BlockReplicaList[j].BlockSize = nn.blockToLocation[bname][j].fileSize
	//		blockReplicaLists[i].BlockReplicaList[j].ReplicaID = int64(nn.blockToLocation[bname][j].replicaID)
	//		//不太理解state，下面这个设置注释掉了
	//		if state := nn.blockToLocation[bname][j].state; state == ReplicaCommitted {
	//			blockReplicaLists[i].BlockReplicaList[j].ReplicaState = proto.BlockLocation_ReplicaCommitted
	//		} else {
	//			blockReplicaLists[i].BlockReplicaList[j].ReplicaState = proto.BlockLocation_ReplicaPending
	//		}
	//	}
	//
	//}
	fmt.Println("======fileList======")
	for _, meta := range nn.fileList {
		fmt.Println(meta)
	}
	fmt.Println("======file2Block======")
	for k, v := range nn.fileToBlock {
		fmt.Println(k, v)
	}
	fmt.Println("======block2Location======")
	for k1, v1 := range nn.blockToLocation {
		fmt.Println(k1, v1)
	}
	var arr = proto.FileLocationArr{FileBlocksList: blockReplicaLists}
	log.Println(len(arr.FileBlocksList))
	log.Println(len(arr.FileBlocksList[0].BlockReplicaList))
	log.Println(arr.FileBlocksList[0].BlockReplicaList[0])
	return &arr, nil
}

func (nn *NameNode) WriteLocation(name string, num int64) (*proto.FileLocationArr, error) {
	var path = name
	timestamp := time.Now().Unix()
	//校验路径是否合法且存在
	for {
		if path == "/" || path == "" {
			break
		}
		index := strings.LastIndex(path, "/")
		path = path[:index]
		if path == "" {
			break
		}
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
	// 每个分片在随机存储在四个不同的可用服务器上
	for i := 0; i < int(num); i++ {
		replicaIndex, err := nn.selectDN(i, nn.replicationFactor, dnNum)
		if err != nil {
			return nil, err
		}
		replicaList := []*proto.BlockLocation{}
		// 每个block存在副本的位置信息
		for j, index := range replicaIndex {
			replicaList = append(replicaList, &proto.BlockLocation{
				IpAddr:       nn.datanodeList[index].IPAddr,
				BlockName:    fmt.Sprintf("%v%v%v%v%v", name, "_", timestamp, "_", j),
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
func (nn *NameNode) selectDN(seedFactor, needNum, section int) ([]int, error) {
	//存放结果的slice
	nums := make([]int, 0)
	// 已选集合,用来去重
	checkSet := make(map[int]interface{})
	// 不可选集合,用来判断失败
	failServer := make(map[int]interface{})
	//随机数生成器，加入时间戳保证每次生成的随机数不一样
	seed := time.Now().UnixNano()
	r := rand.New(rand.NewSource(seed + int64(seedFactor)))
	for len(nums) < needNum {
		//生成随机数
		num := r.Intn(section)
		//fmt.Println("生成随机数:", num)
		// 如果没有被选择过
		if _, ok := checkSet[num]; !ok {
			//且空间足够
			//fmt.Println(num, "没有被选择过")
			if nn.datanodeList[num].DiskUsage > uint64(blockSize) {
				nums = append(nums, num)
				checkSet[num] = nil
				continue
			}
			failServer[num] = nil
		}
		// 如果凑不齐需要的副本,则返回创建错误
		if len(failServer) > section-needNum+1 {
			return nil, public.ErrNotEnoughStorageSpace
		}
	}
	return nums, nil
}
