package service

import (
	"faydfs/config"
	"sync"
	"time"
)

var (
	//默认1min 当前时间 - lastUpdate > softLimit 则允许其他client抢占该Client持有的filepath  (防止用户死亡)
	softLimit int = config.GetConfig().LeaseSoftLimit
	//默认1hour 当前时间 - lastUpdate > hardLimit 则允许LeaseManger强制讲该租约回收销毁 , 考虑文件关闭异常
	hardLimit int = config.GetConfig().LeaseSoftLimit
)

type lease struct {
	holder     string
	lastUpdate int64
	paths      *[]string
}

type LeaseManager struct {
	fileToMetaMap map[string]lease
	mu            sync.Mutex
}

// GetNewLeaseManager 获取LM实例
func GetNewLeaseManager() *LeaseManager {
	fileToMetaMap := make(map[string]lease)
	lm := LeaseManager{
		fileToMetaMap: fileToMetaMap,
	}
	go lm.monitor()
	return &lm
}

// 监视租约是否过期
func (lm *LeaseManager) monitor() {
	for {
		delay := 5 * time.Minute
		time.Sleep(delay)
		lm.mu.Lock()
		for file, fileMeta := range lm.fileToMetaMap {
			//比较是否超过了规定的HardLimit
			if time.Since(time.Unix(fileMeta.lastUpdate, 0)) > (time.Duration(hardLimit) * time.Millisecond) {
				lm.Revoke(fileMeta.holder, file)
			}
		}
		lm.mu.Unlock()
	}
}

// Grant 授予租约
func (lm *LeaseManager) Grant(client string, file string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	_, present := lm.fileToMetaMap[file]
	if present {
		return false
	}
	meta := lease{
		holder:     client,
		lastUpdate: time.Now().Unix(),
	}
	lm.fileToMetaMap[file] = meta
	return true
}

// HasLock 给客户端读权限之前检查该文件是否有别人占用租约
func (lm *LeaseManager) HasLock(file string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lease, present := lm.fileToMetaMap[file]
	if present {
		if time.Since(time.Unix(lease.lastUpdate, 0)) > (time.Duration(softLimit) * time.Millisecond) {
			lm.Revoke(lease.holder, file)
			return false
		}
		return true
	}
	return false
}

// Revoke 取消租约,当client写成功之后,在修改meta的同时放掉租约
func (lm *LeaseManager) Revoke(client string, file string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	delete(lm.fileToMetaMap, file)
}

// Renew 更新租约
func (lm *LeaseManager) Renew(client string, file string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	fileMeta, present := lm.fileToMetaMap[file]
	if present {
		if fileMeta.holder == client {
			meta := lease{holder: client, lastUpdate: time.Now().Unix()}
			lm.fileToMetaMap[file] = meta
			return true
		}
		return lm.Grant(client, file)
	}
	return false
}
