package service

import (
	"context"
	"faydfs/config"
	"faydfs/proto"
	"faydfs/public"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"strings"
	"sync"
	"time"
)

// Block uhb
type blockMeta struct {
	BlockName string
	TimeStamp int64
	BlockID   int
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
	HeartbeatTimeStamp int64
	Status             datanodeStatus
}

// FileMeta 文件元数据
type FileMeta struct {
	FileName      string
	FileSize      uint64
	ChildFileList map[string]*FileMeta
	IsDir         bool

	Blocks []blockMeta
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

var (
	heartbeatTimeout = config.GetConfig().NameNode.HeartbeatTimeout
	blockSize        = config.GetConfig().Block.BlockSize
)

type NameNode struct {
	// fileToBlock data needs to be persisted in disk
	// for recovery of namenode
	//fileToBlock map[string][]blockMeta
	//file2Block *FileToBlock
	DB *DB
	// blockToLocation is not necessary to be in disk
	// blockToLocation can be obtained from datanode blockreport()
	blockToLocation map[string][]replicaMeta

	// datanodeList contains list of datanode ipAddr
	//datanodeList []DatanodeMeta
	//dnList *DatanodeList
	//fileList          map[string]*FileMeta
	//files             *FileList
	blockSize         int64
	replicationFactor int
	lock              sync.RWMutex
}

func (nn *NameNode) ShowLog() {
	dnList := nn.DB.GetDn()
	for _, v := range dnList {
		log.Printf("ip: %v", v.IPAddr)
		log.Printf("status: %v\n", v.Status)
	}
}

func GetNewNameNode(blockSize int64, replicationFactor int) *NameNode {
	namenode := &NameNode{
		//file2Block:        file2BlockDB,
		blockToLocation:   make(map[string][]replicaMeta),
		DB:                GetDB("DB/leveldb"),
		blockSize:         blockSize,
		replicationFactor: replicationFactor,
	}
	namenode.DB.Put("/", &FileMeta{FileName: "/", IsDir: true, ChildFileList: map[string]*FileMeta{}})
	namenode.DB.AddDn(map[string]*DatanodeMeta{})
	go namenode.heartbeatMonitor()
	namenode.getBlockReport2DN()
	return namenode
}

// RegisterDataNode 注册新的dn
func (nn *NameNode) RegisterDataNode(datanodeIPAddr string, diskUsage uint64) {
	nn.lock.Lock()
	defer nn.lock.Unlock()
	meta := DatanodeMeta{
		IPAddr:             datanodeIPAddr,
		DiskUsage:          diskUsage,
		HeartbeatTimeStamp: time.Now().Unix(),
		Status:             datanodeUp,
	}
	// meta.heartbeatTimeStamp = time.Now().Unix()
	//nn.datanodeList = append(nn.datanodeList, meta)

	dnList := nn.DB.GetDn()
	dnList[datanodeIPAddr] = &meta
	if _, ok := dnList[datanodeIPAddr]; !ok {
		nn.DB.AddDn(dnList)
	} else {
		nn.DB.UpdateDn(dnList)
	}
}

// RenameFile 更改路径名称
func (nn *NameNode) RenameFile(src, des string) error {
	if src == "/" {
		return public.ErrCanNotChangeRootDir
	}
	nn.lock.Lock()
	defer nn.lock.Unlock()
	//srcName, ok := nn.fileToBlock[src]
	srcName, ok := nn.DB.Get(src)
	if !ok {
		return public.ErrFileNotFound
	}
	//todo 由于键值存储的原因,rename非空目录需要递归修改子目录path,暂不支持rename非空目录
	if srcName.IsDir && len(srcName.ChildFileList) != 0 {
		return public.ErrOnlySupportRenameEmptyDir
	}
	nn.DB.Put(des, srcName)

	//nn.fileList[des] = nn.fileList[src]
	//srcMeta, _ := nn.files.Get(src)
	//nn.files.Put(des, srcMeta)
	//delete(nn.fileToBlock, src)
	nn.DB.Delete(src)
	//delete(nn.fileList, src)
	//nn.files.Delete(src)
	if src != "/" {
		index := strings.LastIndex(src, "/")
		parentPath := src[:index]
		// 例如 /cfc 父目录经过上述运算 => "",需要额外补上/
		if parentPath == "" {
			parentPath = "/"
		}
		//srcSize, _ := nn.fileList[parentPath].ChildFileList[src]
		//delete(nn.fileList[parentPath].ChildFileList, src)
		fileMeta := nn.DB.GetValue(parentPath)
		newParent := &FileMeta{
			FileName:      fileMeta.FileName,
			FileSize:      fileMeta.FileSize,
			ChildFileList: fileMeta.ChildFileList,
			IsDir:         fileMeta.IsDir,
		}
		newParent.ChildFileList[des] = srcName
		delete(newParent.ChildFileList, src)
		nn.DB.Put(parentPath, newParent)
		//delete(nn.files.GetValue(parentPath).ChildFileList, src)
		//nn.fileList[parentPath].ChildFileList[des] = srcSize
	}
	return nil
}

func (nn *NameNode) FileStat(path string) (*FileMeta, bool) {
	nn.lock.RLock()
	defer nn.lock.RUnlock()
	meta, ok := nn.DB.Get(path)
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
		if _, ok := nn.DB.Get(path); !ok {
			return false, public.ErrPathNotFind
		}
	}
	nn.lock.Lock()
	defer nn.lock.Unlock()
	//判断目录是否已存在
	if _, ok := nn.DB.Get(name); ok {
		return false, public.ErrDirAlreadyExists
	}
	newDir := &FileMeta{
		IsDir:         true,
		ChildFileList: map[string]*FileMeta{},
	}
	nn.DB.Put(name, newDir)
	// 在父目录中追修改子文件
	if name != "/" {
		index := strings.LastIndex(name, "/")
		parentPath := name[:index]
		if parentPath == "" {
			parentPath = "/"
		}
		//nn.fileList[parentPath].ChildFileList[name] = 0
		fileMeta := nn.DB.GetValue(parentPath)
		newParent := &FileMeta{
			FileName:      fileMeta.FileName,
			FileSize:      fileMeta.FileSize,
			ChildFileList: fileMeta.ChildFileList,
			IsDir:         fileMeta.IsDir,
		}
		newParent.ChildFileList[name] = newDir
		nn.DB.Put(parentPath, newParent)
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
		//if _, ok := nn.fileList[path]; !ok {
		if _, ok := nn.DB.Get(path); !ok {
			return false, public.ErrPathNotFind
		}
	}
	nn.lock.Lock()
	defer nn.lock.Unlock()
	// 判断是否为目录文件
	//if meta, ok := nn.fileList[name]; !ok {
	if meta, ok := nn.DB.Get(name); !ok {
		return false, public.ErrPathNotFind
	} else if meta.IsDir { //存在且为目录文件
		//判断目录中是否有其他文件
		if len(meta.ChildFileList) > 0 {
			return false, public.ErrNotEmptyDir
		}
	}
	//路径指定为非目录
	//delete(nn.fileToBlock, name)
	//delete(nn.fileList, name)
	nn.DB.Delete(name)
	// 在父目录中追修改子文件
	if name != "/" {
		index := strings.LastIndex(name, "/")
		parentPath := name[:index]
		if parentPath == "" {
			parentPath = "/"
		}
		fileMeta := nn.DB.GetValue(parentPath)
		newParent := &FileMeta{
			FileName:      fileMeta.FileName,
			FileSize:      fileMeta.FileSize,
			ChildFileList: fileMeta.ChildFileList,
			IsDir:         fileMeta.IsDir,
		}
		delete(newParent.ChildFileList, name)
		nn.DB.Put(parentPath, newParent)

		go func() {
			//todo 额外起一个协程标记删除dn中的数据
		}()
		// 删除父目录中记录的文件
		//deleteSize, _ := nn.fileList[parentPath].ChildFileList[name]
		//delete(nn.fileList[parentPath].ChildFileList, name)
		//srcSize := nn.fileList[parentPath].FileSize
		// 更改父目录的大小
		//nn.fileList[parentPath].FileSize = srcSize - deleteSize
	}
	return true, nil
}

// GetDirMeta 获取目录元数据
func (nn *NameNode) GetDirMeta(name string) ([]*FileMeta, error) {
	resultList := []*FileMeta{}
	nn.lock.Lock()
	defer nn.lock.Unlock()

	//if dir, ok := nn.fileList[name]; ok && dir.IsDir {
	if dir, ok := nn.DB.Get(name); ok && dir.IsDir { // 如果路径存在且对应文件为目录
		for k, _ := range dir.ChildFileList {
			//fileMeta := nn.fileList[k]
			fileMeta := nn.DB.GetValue(k)
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
	log.Println("========== heartbeatMonitor start ==========")
	for {
		heartbeatTimeoutDuration := time.Second * time.Duration(heartbeatTimeout)
		time.Sleep(heartbeatTimeoutDuration)
		dnList := nn.DB.GetDn()
		for ip, datanode := range nn.DB.GetDn() {
			if time.Since(time.Unix(datanode.HeartbeatTimeStamp, 0)) > heartbeatTimeoutDuration {
				go func(ip string) {
					//downDN := nn.dnList.GetValue(i)
					downDN := dnList[ip]
					if downDN.Status == datanodeDown {
						return
					}
					newStateDN := &DatanodeMeta{
						IPAddr:             downDN.IPAddr,
						DiskUsage:          downDN.DiskUsage,
						HeartbeatTimeStamp: downDN.HeartbeatTimeStamp,
						Status:             datanodeDown,
					}
					dnList[ip] = newStateDN
					nn.DB.UpdateDn(dnList)
					log.Println("====== dn :", downDN.IPAddr, " was down ======")
					log.Println("=========================================================================================")
					log.Println("=========================================================================================")
					log.Println("=========================================================================================")
					downBlocks, newIP, processIP, err := nn.reloadReplica(downDN.IPAddr)
					fmt.Println("after reloadReplica")
					if err != nil {
						log.Println("can not reloadReplica: ", err)
						return
					}
					fmt.Println(len(downBlocks))
					for j, downBlock := range downBlocks {
						fmt.Println("循环: ", j, downBlock, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
						err := datanodeReloadReplica(downBlocks[j], newIP[j], processIP[j])
						log.Println("==========block :", downBlock, " on datanode: ", downDN, " was Transferred to datanode: ", newIP[j], "===================")
						if err != nil {
							fmt.Println("================================== transfer err ============================================================")
							log.Println(err)
							return
						}
						//todo 更新blockToLocation
					}
				}(ip)
			}
		}
		//id, datanode := nn.dnList.Range()
		//log.Println("id:", len(id))
		//for i := 0; i < len(id); i++ {
		//	if time.Since(time.Unix(datanode[id[i]].HeartbeatTimeStamp, 0)) > heartbeatTimeoutDuration {
		//		go func(i int) {
		//			//downDN := nn.dnList.GetValue(i)
		//			downDN := datanode[id[i]]
		//			if downDN.Status == datanodeDown {
		//				return
		//			}
		//			newStateDN := &DatanodeMeta{
		//				IPAddr:             downDN.IPAddr,
		//				DiskUsage:          downDN.DiskUsage,
		//				HeartbeatTimeStamp: downDN.HeartbeatTimeStamp,
		//				Status:             datanodeDown,
		//			}
		//			nn.dnList.Update(id[i], newStateDN)
		//			log.Println("====== dn :", downDN.IPAddr, " was down ======")
		//			log.Println("=========================================================================================")
		//			log.Println("=========================================================================================")
		//			log.Println("=========================================================================================")
		//			downBlocks, newIP, processIP, err := nn.reloadReplica(downDN.IPAddr)
		//			fmt.Println("after reloadReplica")
		//			if err != nil {
		//				log.Println("can not reloadReplica: ", err)
		//				return
		//			}
		//			fmt.Println(len(downBlocks))
		//			for j, downBlock := range downBlocks {
		//				fmt.Println("循环: ", j, downBlock, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
		//				err := datanodeReloadReplica(downBlocks[j], newIP[j], processIP[j])
		//				log.Println("==========block :", downBlock, " on datanode: ", downDN, " was Transferred to datanode: ", newIP[j], "===================")
		//				if err != nil {
		//					fmt.Println("================================== transfer err ============================================================")
		//					log.Println(err)
		//					return
		//				}
		//			}
		//		}(i)
		//	}
		//}
	}
}

func (nn *NameNode) Heartbeat(datanodeIPAddr string, diskUsage uint64) {
	//for id, datanode := range nn.datanodeList {
	//	if datanode.IPAddr == datanodeIPAddr {
	//		fmt.Println("update dn :", datanodeIPAddr, "diskUsage :", diskUsage)
	//		nn.datanodeList[id].HeartbeatTimeStamp = time.Now().Unix()
	//		nn.datanodeList[id].DiskUsage = diskUsage
	//	}
	//}

	dnList := nn.DB.GetDn()
	for ip, _ := range dnList {
		if ip == datanodeIPAddr {
			log.Println("update dn:", ip, " ", datanodeIPAddr, "diskUsage :", diskUsage)
			newStateDN := &DatanodeMeta{
				IPAddr:             ip,
				DiskUsage:          diskUsage,
				HeartbeatTimeStamp: time.Now().Unix(),
				Status:             datanodeUp,
			}
			dnList[ip] = newStateDN
			nn.DB.UpdateDn(dnList)
			return
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
	fmt.Println("=========================blockReport=========================")
	fmt.Println(meta)
	fmt.Println("=========================blockReport=========================")
	nn.blockToLocation[blockName] = append(nn.blockToLocation[blockName], meta)
	return
}

func (nn *NameNode) PutSuccess(path string, fileSize uint64, arr *proto.FileLocationArr) {
	var blockList []blockMeta
	// 循环遍历每个block
	nn.lock.Lock()
	defer nn.lock.Unlock()
	for i, list := range arr.FileBlocksList {
		//blockName := fmt.Sprintf("%v%v%v", path, "_", i)
		bm := blockMeta{
			BlockName: list.BlockReplicaList[i].BlockName,
			TimeStamp: time.Now().UnixNano(),
			BlockID:   i,
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
	//nn.fileToBlock[path] = blockList
	//nn.file2Block.Put(path, blockList)
	//nn.fileList[path] = &FileMeta{
	//	FileName:      path,
	//	FileSize:      fileSize,
	//	ChildFileList: nil,
	//	IsDir:         false,
	//}
	newFile := &FileMeta{
		FileName:      path,
		FileSize:      fileSize,
		ChildFileList: nil,
		IsDir:         false,
		Blocks:        blockList,
	}
	nn.DB.Put(path, newFile)
	// 在父目录中追加子文件
	if path != "/" {
		index := strings.LastIndex(path, "/")
		parentPath := path[:index]
		if parentPath == "" {
			parentPath = "/"
		}
		fileMeta := nn.DB.GetValue(parentPath)
		newParent := &FileMeta{
			FileName:      fileMeta.FileName,
			FileSize:      fileMeta.FileSize,
			ChildFileList: fileMeta.ChildFileList,
			IsDir:         fileMeta.IsDir,
		}
		newParent.ChildFileList[path] = newFile
		nn.DB.Put(parentPath, newParent)
		//srcSize := nn.fileList[parentPath].FileSize
		//// 更改父目录的大小
		//nn.fileList[parentPath].FileSize = srcSize + fileSize
		//nn.fileList[parentPath].ChildFileList[path] = fileSize

	}
}

func (nn *NameNode) GetLocation(name string) (*proto.FileLocationArr, error) {

	blockReplicaLists := []*proto.BlockReplicaList{}
	//if block, ok := nn.fileToBlock[name]; !ok {
	if block, ok := nn.DB.Get(name); !ok {
		return nil, public.ErrPathNotFind
	} else {
		for _, meta := range block.Blocks {
			// 每个block存在副本的位置信息
			if replicaLocation, exit := nn.blockToLocation[meta.BlockName]; !exit {
				return nil, public.ErrReplicaNotFound
			} else {
				replicaList := []*proto.BlockLocation{}
				for _, location := range replicaLocation {
					var state proto.BlockLocation_ReplicaMetaState
					if location.state == ReplicaPending {
						state = proto.BlockLocation_ReplicaPending
					} else {
						state = proto.BlockLocation_ReplicaCommitted
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

	var arr = proto.FileLocationArr{FileBlocksList: blockReplicaLists}
	return &arr, nil
}

func (nn *NameNode) WriteLocation(name string, num int64) (*proto.FileLocationArr, error) {
	var path = name
	timestamp := time.Now().UnixNano()
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
		//if _, ok := nn.fileList[path]; !ok {
		if _, ok := nn.DB.Get(path); !ok {
			return nil, public.ErrPathNotFind
		}
	}
	nn.lock.Lock()
	defer nn.lock.Unlock()
	//判断目标文件是否已存在
	if _, ok := nn.DB.Get(name); ok {
		return nil, public.ErrDirAlreadyExists
	}
	fileArr := proto.FileLocationArr{}
	blocks := []*proto.BlockReplicaList{}

	// 一共需要num * replicationFactor个块 (最少切片块数 * 副本数)
	// 每个分片在随机存储在四个不同的可用服务器上
	for i := 0; i < int(num); i++ {
		replicaIndex, err := nn.selectDN(nn.replicationFactor)
		if err != nil {
			return nil, err
		}
		replicaList := []*proto.BlockLocation{}
		// 每个block存在副本的位置信息
		for j, dn := range replicaIndex {
			fmt.Println("=============================================================")
			fmt.Println("choose DNList: ", replicaIndex)
			fmt.Println("choose DN: ", dn)
			realNameIndex := strings.LastIndex(name, "/")
			replicaList = append(replicaList, &proto.BlockLocation{
				IpAddr:       dn.IPAddr,
				BlockName:    fmt.Sprintf("%v%v%v%v%v", name[realNameIndex+1:], "_", timestamp, "_", i),
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

// needNum: 需要的副本数量
// 采用map range时的不定起点作为随机选择方式,减少了之前大量的循环操作
func (nn *NameNode) selectDN(needNum int) ([]*DatanodeMeta, error) {
	//存放结果的slice
	result := []*DatanodeMeta{}

	dnList := nn.DB.GetDn()
	for _, dn := range dnList {
		if dn.DiskUsage > uint64(blockSize) && dn.Status != datanodeDown {
			result = append(result, dn)
		}
	}
	if len(result) < needNum {
		return nil, public.ErrNotEnoughStorageSpace
	}
	return result, nil
}

func (nn *NameNode) selectTransferDN(disableIP []string) (string, error) {
	//目标DN
	fmt.Println("======================================================选择备份转移节点======================================================")
	dnList := nn.DB.GetDn()
outer:
	for ip, dn := range dnList {
		if dn.DiskUsage > uint64(blockSize) && dn.Status != datanodeDown {
			//且不是已拥有该block的dn
			for _, disIp := range disableIP {
				if disIp == ip {
					continue outer
				}
			}
			fmt.Println("找到可用IP: ", ip)
			return ip, nil
		}
	}
	return "", public.ErrNotEnoughStorageSpace
}

// blockName and newIP
func (nn *NameNode) reloadReplica(downIp string) ([]string, []string, []string, error) {
	fmt.Println("into reloadReplica")
	downBlocks := []string{}
	newIP := []string{}
	processIP := []string{}
	//找到down掉的ip地址中所有的block
	for _, location := range nn.blockToLocation {
		for i, meta := range location {
			//找到存储在downIp中的block
			if meta.ipAddr == downIp {
				//添加到待转移副本切片
				downBlocks = append(downBlocks, meta.blockName)
				//挑选其他副本的dn
				fmt.Println("down blockName: ", meta.blockName)
				replicaMetas := nn.blockToLocation[meta.blockName]
				// 不可作为副本存放的新节点的节点
				disableIP := []string{}
				fmt.Println(replicaMetas)
				for _, meta := range replicaMetas {
					disableIP = append(disableIP, meta.ipAddr)
				}
				fmt.Println("disabeIP: ", disableIP)
				dnIP, err := nn.selectTransferDN(disableIP)
				if err != nil {
					return nil, nil, nil, err
				}
				newIP = append(newIP, dnIP)
				if i != 0 {
					processIP = append(processIP, location[i-1].ipAddr)
				} else {
					processIP = append(processIP, location[i+1].ipAddr)
				}
			}
		}
	}
	fmt.Println("downBlocks:", downBlocks)
	fmt.Println("newIps:", newIP)
	fmt.Println("processIP:", processIP)
	return downBlocks, newIP, processIP, nil
}

func datanodeReloadReplica(blockName, newIP, processIP string) error {
	conn, client, _, _ := getGrpcN2DConn(processIP)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	//status, err := (*client).GetDirMeta(ctx, &proto.PathName{PathName: remoteDirPath})
	log.Println("replicate "+blockName+" to ", newIP)
	_, err := (*client).ReloadReplica(ctx, &proto.CopyReplica2DN{BlockName: blockName, NewIP: newIP})
	if err != nil {
		log.Print("datanode ReloadReplica fail: processIP :", err)
		return err
	}
	return nil
}

func (nn *NameNode) getBlockReport2DN() {
	dnList := nn.DB.GetDn()
	for ip, dn := range dnList {
		if dn.Status != datanodeDown {
			blockReplicaList, err := nn.getBlockReportRPC(ip)
			if err != nil {
				log.Println(err)
				return
			}
			for _, bm := range blockReplicaList.BlockReplicaList {
				nn.GetBlockReport(bm)
			}
		}
	}

}

func (nn *NameNode) getBlockReportRPC(addr string) (*proto.BlockReplicaList, error) {
	conn, client, _, _ := getGrpcN2DConn(addr)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	//status, err := (*client).GetDirMeta(ctx, &proto.PathName{PathName: remoteDirPath})
	blockReplicaList, err := (*client).GetBlockReport(ctx, &proto.Ping{Ping: addr})
	if err != nil {
		log.Print("datanode get BlockReport fail: addr :", addr)
		return nil, err
	}
	return blockReplicaList, nil
}

func getGrpcN2DConn(address string) (*grpc.ClientConn, *proto.N2DClient, *context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//conn, err := grpc.DialContext(ctx, address, grpc.WithBlock())
	conn2, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect to %v error %v", address, err)
	}
	client := proto.NewN2DClient(conn2)
	return conn2, &client, &cancel, err
}
