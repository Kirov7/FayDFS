package service

import (
	"sync"
	"time"
)

type lease struct {
	holder     string
	lastUpdate int64
	paths      *[]string
	softLimit  int //默认1min 当前时间 - lastUpdate > softLimit 则允许其他client抢占该Client持有的filepath
	hardLimit  int //默认1hour 当前时间 - lastUpdate > hardLimit 则允许LeaseManger强制讲该租约回收销毁
}

type LeaseManager struct {
	fileToMetaMap map[string]lease
	mu            sync.Mutex
}

// GetNewLeaseManager 获取LM实例
func GetNewLeaseManager() *LeaseManager {
	fileToMetaMap := make(map[string]lease)
	lm := LeaseManager{fileToMetaMap: fileToMetaMap}
	go lm.monitor()
	return &lm
}

// 监视租约是否过期
func (lm *LeaseManager) monitor() {
	delay := 5 * time.Minute
	time.Sleep(delay)
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for file, fileMeta := range lm.fileToMetaMap {
		//todo 此处应使用hardLimit来限制
		if time.Since(time.Unix(fileMeta.lastUpdate, 0)) > delay {
			lm.revoke(fileMeta.holder, file)
		}
	}
	lm.monitor()
}

// Grant 授予租约
func (lm *LeaseManager) Grant(client string, file string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	_, present := lm.fileToMetaMap[file]
	if present {
		return false
	}
	meta := lease{holder: client, lastUpdate: time.Now().Unix()}
	lm.fileToMetaMap[file] = meta
	return true
}

// HasLock 给客户端读权限之前检查该文件是否有别人占用租约
func (lm *LeaseManager) HasLock(file string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	//todo 检查softLimit是否可以取消租约
	_, present := lm.fileToMetaMap[file]
	if present {
		return true
	}
	return false
}

// 取消租约
func (lm *LeaseManager) revoke(client string, file string) {
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
		return false
	}
	return false
}
