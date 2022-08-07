package service

type Result struct {
	ResultCode     int         `json:"code"`
	ResultExtraMsg string      `json:"extra_msg"`
	Data           interface{} `json:"data"`
}

type clientAPI interface {
	Put(localFilePath, remoteFilePath string) Result
	Get(remoteFilePath, localFilePath string) Result
	Delete(remoteFilePath string) Result
	Stat(remoteFilePath string) Result
	Rename(renameSrcPath, renameDestPath string) Result
	Mkdir(remoteFilePath string) Result
	List(remoteDirPath string) Result
}
