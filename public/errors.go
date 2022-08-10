package public

import "errors"

const (
	CAN_NOT_FIND_REAL = 0
)

var ErrRealIPNotFound = errors.New("file not found")
