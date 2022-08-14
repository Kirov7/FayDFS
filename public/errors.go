package public

import "errors"

const (
	CAN_NOT_FIND_REAL = 0
)

var ErrRealIPNotFound = errors.New("IP not found")
var ErrFileNotFound = errors.New("file not found")
var ErrPathNotFind = errors.New("path not found")
var ErrDirAlreadyExists = errors.New("the directory already exists")
var ErrNotEmptyDir = errors.New("other files in directory")
var ErrNotDir = errors.New("the files is not directory")
