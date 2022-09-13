package service

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"faydfs/config"
	"faydfs/namenode/raft"
	"faydfs/proto"
	"faydfs/public"
	"fmt"
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

type DBEntry struct {
	typeMode typeMode
	key      []byte
	value    interface{}
}

type datanodeStatus string
type replicaState string
type typeMode int

// namenode constants
const (
	datanodeDown     = datanodeStatus("datanodeDown")
	datanodeUp       = datanodeStatus("datanodeUp")
	ReplicaPending   = replicaState("pending")
	ReplicaCommitted = replicaState("committed")
)

const (
	PUT_FILE = typeMode(iota)
	DEL_FILE
	ADD_DN
	UPDATE_DN
)

var (
	heartbeatTimeout = config.GetConfig().NameNode.HeartbeatTimeout
	blockSize        = config.GetConfig().Block.BlockSize
)

type NameNode struct {
	DB *DB
	// blockToLocation is not necessary to be in disk
	// blockToLocation can be obtained from datanode blockreport()
	blockToLocation map[string][]replicaMeta

	blockSize         int64
	replicationFactor int

	Rf          *raft.Raft
	applyCh     chan *proto.ApplyMsg
	notifyChans map[int64]chan bool
	//stm         TopoConfigSTM
	confStm     map[string]string
	stopApplyCh chan interface{}
	lastApplied int

	lock sync.RWMutex
}

func (nn *NameNode) ShowLog() {
	dnList := nn.DB.GetDn()
	for _, v := range dnList {
		log.Printf("ip: %v", v.IPAddr)
		log.Printf("status: %v\n", v.Status)
	}
}

func GetNewNameNode(nodes map[int]string, nodeId int, blockSize int64, replicationFactor int) *NameNode {

	raftClientEnds := []*raft.RaftClientEnd{}
	for nodeId, nodeAddr := range nodes {
		newEnd := raft.MakeRaftClientEnd(nodeAddr, uint64(nodeId))
		raftClientEnds = append(raftClientEnds, newEnd)
	}

	newApplyCh := make(chan *proto.ApplyMsg)
	newMetaDB := GetDB(fmt.Sprintf("DB/log_%d", nodeId))
	newLogDB := GetDB(fmt.Sprintf("DB/meta_%d", nodeId))
	newRf := raft.BuildRaft(raftClientEnds, nodeId, *newLogDB, newApplyCh, 500, 1500)

	namenode := &NameNode{
		blockToLocation:   make(map[string][]replicaMeta),
		DB:                newMetaDB,
		blockSize:         blockSize,
		replicationFactor: replicationFactor,
		Rf:                newRf,
		applyCh:           newApplyCh,
	}
	namenode.stopApplyCh = make(chan interface{})
	namenode.restoreSnapshot(newRf.ReadSnapshot())

	go namenode.ApplingToSTM(namenode.stopApplyCh)

	if _, ok := namenode.DB.Get("/"); !ok {
		//namenode.DB.Put("/", &FileMeta{FileName: "/", IsDir: true, ChildFileList: map[string]*FileMeta{}})
		//namenode.DB.AddDn(map[string]*DatanodeMeta{})
		entrys := []DBEntry{DBEntry{
			typeMode: PUT_FILE,
			key:      []byte("/"),
			value:    &FileMeta{FileName: "/", IsDir: true, ChildFileList: map[string]*FileMeta{}},
		}, {
			typeMode: ADD_DN,
			key:      public.DN_LIST_KEY,
			value:    map[string]*DatanodeMeta{},
		}}
		namenode.Propose(public.EncodeData(entrys))
	}

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

	dnList := nn.DB.GetDn()
	dnList[datanodeIPAddr] = &meta
	if _, ok := dnList[datanodeIPAddr]; !ok {
		entrys := []DBEntry{DBEntry{
			typeMode: ADD_DN,
			key:      public.DN_LIST_KEY,
			value:    dnList,
		}}
		//nn.DB.AddDn(dnList)
		nn.Propose(public.EncodeData(entrys))
	} else {
		entrys := []DBEntry{DBEntry{
			typeMode: UPDATE_DN,
			key:      public.DN_LIST_KEY,
			value:    dnList,
		}}
		//nn.DB.UpdateDn(dnList)
		nn.Propose(public.EncodeData(entrys))
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
	entrys := []DBEntry{}

	//nn.DB.Put(des, srcName)
	entrys = append(entrys, DBEntry{
		typeMode: PUT_FILE,
		key:      []byte(des),
		value:    srcName,
	})
	//nn.DB.Delete(src)
	entrys = append(entrys, DBEntry{
		typeMode: DEL_FILE,
		key:      []byte(src),
		value:    nil,
	})
	if src != "/" {
		index := strings.LastIndex(src, "/")
		parentPath := src[:index]
		// 例如 /cfc 父目录经过上述运算 => "",需要额外补上/
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
		newParent.ChildFileList[des] = srcName
		delete(newParent.ChildFileList, src)
		//nn.DB.Put(parentPath, newParent)
		entrys = append(entrys, DBEntry{
			typeMode: PUT_FILE,
			key:      []byte(parentPath),
			value:    newParent,
		})
	}
	if ok := nn.Propose(public.EncodeData(entrys)); !ok {
		return public.ErrProposeFail
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
	entrys := []DBEntry{}
	//nn.DB.Put(name, newDir)
	entrys = append(entrys, DBEntry{
		typeMode: PUT_FILE,
		key:      []byte(name),
		value:    newDir,
	})
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
		//nn.DB.Put(parentPath, newParent)
		entrys = append(entrys, DBEntry{
			typeMode: PUT_FILE,
			key:      []byte(parentPath),
			value:    newParent,
		})
	}
	if ok := nn.Propose(public.EncodeData(entrys)); !ok {
		return false, public.ErrProposeFail
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
	entrys := []DBEntry{}
	//nn.DB.Delete(name)
	entrys = append(entrys, DBEntry{
		typeMode: DEL_FILE,
		key:      []byte(name),
		value:    nil,
	})
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
		//nn.DB.Put(parentPath, newParent)
		entrys = append(entrys, DBEntry{
			typeMode: PUT_FILE,
			key:      []byte(parentPath),
			value:    newParent,
		})
		go func() {
			//todo 额外起一个协程标记删除dn中的数据
		}()
	}
	if ok := nn.Propose(public.EncodeData(entrys)); !ok {
		return false, public.ErrProposeFail
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
					//nn.DB.UpdateDn(dnList)
					entrys := []DBEntry{{
						typeMode: UPDATE_DN,
						key:      public.DN_LIST_KEY,
						value:    dnList,
					}}
					nn.Propose(public.EncodeData(entrys))
					log.Println("============================================== dn :", downDN.IPAddr, " was down ==============================================")
					downBlocks, newIP, processIP, err := nn.reloadReplica(downDN.IPAddr)
					fmt.Println("after reloadReplica")
					if err != nil {
						log.Println("can not reloadReplica: ", err)
						return
					}
					fmt.Println(len(downBlocks))
					for j, downBlock := range downBlocks {
						err := datanodeReloadReplica(downBlocks[j], newIP[j], processIP[j])
						log.Println("==========block :", downBlock, " on datanode: ", downDN, " was Transferred to datanode: ", newIP[j], "===================")
						if err != nil {
							fmt.Println("================================== transfer err ============================================================")
							log.Println(err)
							return
						}
						go func(blockName, newIP string) {
							nn.lock.Lock()
							defer nn.lock.Unlock()
							nn.blockToLocation[blockName] = append(nn.blockToLocation[blockName], replicaMeta{
								blockName: blockName,
								ipAddr:    newIP,
								state:     ReplicaCommitted,
								replicaID: len(nn.blockToLocation[blockName]),
							})
						}(downBlocks[j], newIP[j])

					}
				}(ip)
			}
		}
	}
}

func (nn *NameNode) Heartbeat(datanodeIPAddr string, diskUsage uint64) {

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
			//nn.DB.UpdateDn(dnList)
			entrys := []DBEntry{{
				typeMode: UPDATE_DN,
				key:      public.DN_LIST_KEY,
				value:    dnList,
			}}
			nn.Propose(public.EncodeData(entrys))
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

func (nn *NameNode) PutSuccess(path string, fileSize uint64, arr *proto.FileLocationArr) bool {
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

	newFile := &FileMeta{
		FileName:      path,
		FileSize:      fileSize,
		ChildFileList: nil,
		IsDir:         false,
		Blocks:        blockList,
	}

	//nn.DB.Put(path, newFile)
	entrys := []DBEntry{}
	newEntry := DBEntry{
		typeMode: PUT_FILE,
		key:      []byte(path),
		value:    newFile,
	}
	entrys = append(entrys, newEntry)
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

		//nn.DB.Put(parentPath, newParent)
		newEntry := DBEntry{
			typeMode: PUT_FILE,
			key:      []byte(parentPath),
			value:    newParent,
		}
		entrys = append(entrys, newEntry)
	}
	nn.lock.Unlock()
	return nn.Propose(public.EncodeData(entrys))
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

func (nn *NameNode) Propose(entrys []byte) (success bool) {
	logIndex, _, isLeader := nn.Rf.Propose(public.EncodeData(entrys))
	if !isLeader {
		return false
	}
	logIndexInt64 := int64(logIndex)
	nn.lock.Lock()
	// make a response chan for sync return result to client
	ch := nn.getRespNotifyChan(logIndexInt64)
	nn.lock.Unlock()
	select {
	case success = <-ch:
		return success
	case <-time.After(time.Second * 3):
		success = false
	}
	go func() {
		nn.lock.Lock()
		delete(nn.notifyChans, logIndexInt64)
		nn.lock.Unlock()
	}()
	return success
}

func (nn *NameNode) restoreSnapshot(snapData []byte) {
	if snapData == nil {
		return
	}
	buf := bytes.NewBuffer(snapData)
	data := gob.NewDecoder(buf)
	var stm map[string]string
	if data.Decode(&stm) != nil {
		log.Println("decode stm data error")
	}
	stmBytes, _ := json.Marshal(stm)
	log.Println("recover stm -> " + string(stmBytes))
	nn.confStm = stm
}

func (nn *NameNode) taskSnapshot(index int) {
	var bytesState bytes.Buffer
	enc := gob.NewEncoder(&bytesState)
	enc.Encode(nn.confStm)
	// snapshot
	nn.Rf.Snapshot(index, bytesState.Bytes())
}

func (nn *NameNode) getRespNotifyChan(logIndex int64) chan bool {
	if _, ok := nn.notifyChans[logIndex]; !ok {
		nn.notifyChans[logIndex] = make(chan bool, 1)
	}
	return nn.notifyChans[logIndex]
}

//todo meta写入第13步
func (nn *NameNode) ApplingToSTM(done <-chan interface{}) {
	for {
		select {
		case <-done:
			return
		case appliedMsg := <-nn.applyCh:
			if appliedMsg.CommandValid {
				req := DecodeData2DBEntrys(appliedMsg.Command)
				resp := false

				for _, entry := range req {

					switch entry.typeMode {
					case PUT_FILE:
						{
							nn.DB.Put(entry.key, entry.value)
						}
					case DEL_FILE:
						{
							nn.DB.Delete(entry.key)
						}

					case UPDATE_DN:
						{
							nn.DB.UpdateDn(entry.value)
						}
					case ADD_DN:
						{
							nn.DB.AddDn(entry.value)
						}
					}

				}

				nn.lastApplied = int(appliedMsg.CommandIndex)
				if nn.Rf.GetLogCount() > 20 {
					nn.taskSnapshot(int(appliedMsg.CommandIndex))
				}
				log.Printf("apply op to meta server stm: %s\n", req)
				nn.lock.Lock()
				ch := nn.getRespNotifyChan(appliedMsg.CommandIndex)
				nn.lock.Unlock()
				ch <- resp
			} else if appliedMsg.SnapshotValid {
				nn.lock.Lock()
				if nn.Rf.CondInstallSnapshot(int(appliedMsg.SnapshotTerm), int(appliedMsg.SnapshotIndex), appliedMsg.Snapshot) {
					log.Printf("restoresnapshot \n")
					nn.restoreSnapshot(appliedMsg.Snapshot)
					nn.lastApplied = int(appliedMsg.SnapshotIndex)
				}
				nn.lock.Unlock()
			}
		}
	}
}

func DecodeData2DBEntrys(in []byte) []DBEntry {
	dec := gob.NewDecoder(bytes.NewBuffer(in))
	req := []DBEntry{}
	dec.Decode(&req)
	return req
}
