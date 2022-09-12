package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"faydfs/namenode/service"
	"faydfs/proto"
	"faydfs/public"
	"log"
)

type RaftPersistenState struct {
	curTerm  int64
	votedFor int64
}

func MakePersistRaftLog(db service.DB) *RaftLog {
	empEnt := &proto.Entry{}
	empEntEncode := EncodeEntry(empEnt)
	db.RaftPut(EncodeRaftLogKey(public.INIT_LOG_INDEX), empEntEncode)
	return &RaftLog{db: db, firstIdx: 0, lastIdx: 0}
}

// ReadRaftState
// read the persist curTerm, votedFor for node from storage engine
func (rfLog *RaftLog) ReadRaftState() (curTerm int64, votedFor int64) {
	rfBytes, err := rfLog.db.RaftGet(public.RAFT_STATE_KEY)
	if err != nil {
		return 0, -1
	}
	rfState := DecodeRaftState(rfBytes)
	return rfState.curTerm, rfState.votedFor
}

func (rfLog *RaftLog) PersisSnapshot(snapContext []byte) {
	rfLog.db.RaftPut(public.SNAPSHOT_STATE_KEY, snapContext)
	// rfLog.dbEng.GetPrefixRangeKvs(consts.RAFTLOG_PREFIX)
}

func (rfLog *RaftLog) ReadSnapshot() ([]byte, error) {
	bytes, err := rfLog.db.RaftGet(public.SNAPSHOT_STATE_KEY)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// GetFirstLogId
// get the first log id from storage engine
func (rfLog *RaftLog) GetFirstLogId() uint64 {
	return rfLog.firstIdx
}

func (rfLog *RaftLog) GetEntry(index int64) *proto.Entry {
	encodeValue, err := rfLog.db.RaftGet(EncodeRaftLogKey(uint64(index)))
	if err != nil {
		log.Println("get log entry with id %d error! fristlog index is %d, lastlog index is %d\n", int64(index), rfLog.firstIdx, rfLog.lastIdx)
		//rfLog.db.GetPrefixRangeKvs(consts.RAFTLOG_PREFIX)
		panic(err)
	}
	return DecodeEntry(encodeValue)
}

// get range log from storage engine, and return the copy
// [lo, hi)
//
func (rfLog *RaftLog) GetRange(lo, hi int64) []*proto.Entry {
	rfLog.mu.RLock()
	defer rfLog.mu.RUnlock()
	ents := []*proto.Entry{}
	for i := lo; i < hi; i++ {
		ents = append(ents, rfLog.GetEntry(i))
	}
	return ents
}

// SetEntFirstTermAndIndex
func (rfLog *RaftLog) SetEntFirstTermAndIndex(term, index int64) error {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	firstIdx := rfLog.GetFirstLogId()
	encodeValue, err := rfLog.db.RaftGet(EncodeRaftLogKey(uint64(firstIdx)))
	if err != nil {
		log.Printf("get log entry with id %d error!", firstIdx)
		panic(err)
	}
	// del olf first ent
	log.Printf("del log index:%d\n", firstIdx)
	if err := rfLog.db.RaftDelete(EncodeRaftLogKey(firstIdx)); err != nil {
		return err
	}
	ent := DecodeEntry(encodeValue)
	ent.Term = uint64(term)
	ent.Index = index
	log.Printf("change first entry to -> " + ent.String())
	newEntEncode := EncodeEntry(ent)
	rfLog.firstIdx, rfLog.lastIdx = uint64(index), uint64(index)
	return rfLog.db.RaftPut(EncodeRaftLogKey(uint64(index)), newEntEncode)
}

// erase after idx, !!!WRANNING!!! is withDel is true, this operation will delete log key[idx:]
// in storage engine
//
func (rfLog *RaftLog) EraseAfter(idx int64, withDel bool) []*proto.Entry {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	firstLogId := rfLog.GetFirstLogId()
	log.Printf("start erase after %d\n", idx)
	if withDel {
		for i := idx; i <= int64(rfLog.GetLastLogId()); i++ {
			if err := rfLog.db.RaftDelete(EncodeRaftLogKey(uint64(i))); err != nil {
				panic(err)
			}
		}
		rfLog.lastIdx = uint64(idx) - 1
	}
	ents := []*proto.Entry{}
	for i := firstLogId; i < uint64(idx); i++ {
		ents = append(ents, rfLog.GetEntry(int64(i)))
	}
	return ents
}

// EraseBefore
// erase log before from idx, and copy [idx:] log return
// this operation don't modity log in storage engine
//
func (rfLog *RaftLog) EraseBefore(idx int64) []*proto.Entry {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	ents := []*proto.Entry{}
	lastLogId := rfLog.GetLastLogId()
	log.Printf("Get log [%d:%d] ", idx, lastLogId)
	for i := idx; i <= int64(lastLogId); i++ {
		ents = append(ents, rfLog.GetEntry(i))
	}
	return ents
}

func (rfLog *RaftLog) EraseBeforeWithDel(idx int64) error {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	firstLogId := rfLog.GetFirstLogId()
	for i := firstLogId; i < uint64(idx); i++ {
		if err := rfLog.db.RaftDelete(EncodeRaftLogKey(i)); err != nil {
			log.Printf("Erase before error\n")
			return err
		}
		log.Printf("del log with id %d success", i)
	}
	// rfLog.dbEng.GetPrefixRangeKvs(consts.RAFTLOG_PREFIX)
	rfLog.firstIdx = uint64(idx)
	log.Printf("After erase log, firstIdx: %d, lastIdx: %d\n", rfLog.firstIdx, rfLog.lastIdx)
	return nil
}

// Append
//  a new entry to raftlog, put it to storage engine
func (rfLog *RaftLog) Append(newEnt *proto.Entry) {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	newEntEncode := EncodeEntry(newEnt)
	err := rfLog.db.RaftPut(EncodeRaftLogKey(uint64(newEnt.Index)), newEntEncode)
	if err != nil {
		panic(err)
	}
	if newEnt.Index > int64(rfLog.lastIdx) {
		rfLog.lastIdx = uint64(newEnt.Index)
	}
	log.Printf("Append entry index:%d to levebdb log\n", newEnt.Index)
}

// LogItemCount
// get total log count from storage engine
func (rfLog *RaftLog) LogItemCount() int {
	rfLog.mu.RLock()
	defer rfLog.mu.RUnlock()
	return int(rfLog.lastIdx) - int(rfLog.firstIdx) + 1
}

// GetLastLogId
// get the last log id from storage engine
func (rfLog *RaftLog) GetLastLogId() uint64 {
	return rfLog.lastIdx
}

// EncodeRaftLogKey
// encode raft log key with perfix -> RAFTLOG_PREFIX
//
func EncodeRaftLogKey(idx uint64) []byte {
	var outBuf bytes.Buffer
	outBuf.Write(public.RAFTLOG_PREFIX)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(idx))
	outBuf.Write(b)
	return outBuf.Bytes()
}

// EncodeEntry
// encode log entry to bytes sequence
func EncodeEntry(ent *proto.Entry) []byte {
	var bytesEnt bytes.Buffer
	enc := gob.NewEncoder(&bytesEnt)
	enc.Encode(ent)
	return bytesEnt.Bytes()
}

// DecodeEntry
// decode log entry from bytes sequence
func DecodeEntry(in []byte) *proto.Entry {
	dec := gob.NewDecoder(bytes.NewBuffer(in))
	ent := proto.Entry{}
	dec.Decode(&ent)
	return &ent
}

// GetLast
//
// get the last entry from storage engine
//
func (rfLog *RaftLog) GetLast() *proto.Entry {
	rfLog.mu.RLock()
	defer rfLog.mu.RUnlock()
	return rfLog.GetEntry(int64(rfLog.lastIdx))
}

// GetFirst
//
// get the first entry from storage engine
//
func (rfLog *RaftLog) GetFirst() *proto.Entry {
	rfLog.mu.RLock()
	defer rfLog.mu.RUnlock()
	return rfLog.GetEntry(int64(rfLog.firstIdx))
}

// PersistRaftState Persistent storage raft state
// (curTerm, and votedFor)
// you can find this design in raft paper figure2 State definition
//
func (rfLog *RaftLog) PersistRaftState(curTerm int64, votedFor int64) {
	rfState := &RaftPersistenState{
		curTerm:  curTerm,
		votedFor: votedFor,
	}
	rfLog.db.RaftPut(public.RAFT_STATE_KEY, EncodeRaftState(rfState))
}

// EncodeRaftState
// encode RaftPersistenState to bytes sequence
func EncodeRaftState(rfState *RaftPersistenState) []byte {
	var bytesState bytes.Buffer
	enc := gob.NewEncoder(&bytesState)
	enc.Encode(rfState)
	return bytesState.Bytes()
}

// DecodeRaftState
// decode RaftPersistenState from bytes sequence
func DecodeRaftState(in []byte) *RaftPersistenState {
	dec := gob.NewDecoder(bytes.NewBuffer(in))
	rfState := RaftPersistenState{}
	dec.Decode(&rfState)
	return &rfState
}

// ReInitLogs
// make logs to init state
func (rfLog *RaftLog) ReInitLogs() error {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	log.Printf("start reinitlogs\n")
	// delete all log
	if err := rfLog.db.RaftDelPrefixKeys(string(public.RAFTLOG_PREFIX)); err != nil {
		return err
	}
	// add a empty
	empEnt := &proto.Entry{}
	empEntEncode := EncodeEntry(empEnt)
	rfLog.firstIdx, rfLog.lastIdx = 0, 0
	return rfLog.db.RaftPut(EncodeRaftLogKey(public.INIT_LOG_INDEX), empEntEncode)
}
