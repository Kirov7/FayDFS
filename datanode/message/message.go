package message

// Message 进程间通信对象
type Message struct {
	Mode      string
	BlockName string
	Content   []byte
	IpAddr    string
}
