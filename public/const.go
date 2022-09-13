package public

var RAFTLOG_PREFIX = []byte{0x11, 0x11, 0x19, 0x96}

var RAFT_STATE_KEY = []byte{0x19, 0x49}

var SNAPSHOT_STATE_KEY = []byte{0x19, 0x97}

const INIT_LOG_INDEX = 0

var DN_LIST_KEY = []byte("dnList")
