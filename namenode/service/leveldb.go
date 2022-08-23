package service

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"log"
	"sync"
)

var DBlock sync.RWMutex

type FileToBlock struct {
	Length int
	DB     *leveldb.DB
}

type FileList struct {
	Length int
	DB     *leveldb.DB
}

// DatanodeList 对外暴露为List结构
type DatanodeList struct {
	Length int
	DB     *leveldb.DB
}

// =========================================================== FileToBlock ============================================================

func GetFileToBlock(dbPath string) *FileToBlock {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	iter := db.NewIterator(nil, nil)
	length := 0
	for iter.Next() {
		length++
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		log.Fatal(err)
	}
	return &FileToBlock{DB: db, Length: length}
}

func (f2b *FileToBlock) GetLength() int {
	return f2b.Length
}

func (f2b *FileToBlock) Put(key string, value []blockMeta) {
	valueBytes := f2b.blockMetas2Bytes(value)
	err := f2b.DB.Put([]byte(key), valueBytes, nil)
	if err != nil {
		log.Fatal(err)
	}
	f2b.Length++
}

func (f2b *FileToBlock) Get(key string) ([]blockMeta, bool) {
	data, err := f2b.DB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, false
		}
		log.Fatal(err)
	}
	result := f2b.bytes2BlockMetas(data)
	return result, true
}

func (f2b *FileToBlock) GetValue(key string) []blockMeta {
	data, err := f2b.DB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil
		}
		log.Fatal(err)
	}
	result := f2b.bytes2BlockMetas(data)
	return result
}

func (f2b *FileToBlock) Delete(key string) {
	err := f2b.DB.Delete([]byte(key), nil)
	if err != nil {
		log.Fatal(err)
	}
	f2b.Length--
}

func (f2b *FileToBlock) Range() ([]string, [][]blockMeta) {
	keys := []string{}
	values := [][]blockMeta{}
	iter := f2b.DB.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, string(key))
		value := iter.Value()
		values = append(values, f2b.bytes2BlockMetas(value))
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Fatal(err)
	}
	return keys, values
}

func (f2b FileToBlock) blockMetas2Bytes(structs []blockMeta) []byte {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(structs)
	if err != nil {
		log.Fatal(err)
	}
	return b.Bytes()
}

func (f2b FileToBlock) bytes2BlockMetas(b []byte) []blockMeta {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	var data []blockMeta
	err := dec.Decode(&data)
	if err != nil {
		log.Fatal("Error decoding GOB data:", err)
	}
	return data
}

// =========================================================== FileList ============================================================

func GetFileList(dbPath string) *FileList {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	iter := db.NewIterator(nil, nil)
	length := 0
	for iter.Next() {
		length++
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		log.Fatal(err)
	}
	return &FileList{DB: db, Length: length}
}

func (fl *FileList) GetLength() int {
	return fl.Length
}

func (fl *FileList) Put(key string, value *FileMeta) {
	valueBytes := fl.fileMetas2Bytes(value)
	err := fl.DB.Put([]byte(key), valueBytes, nil)
	if err != nil {
		log.Fatal(err)
	}
	fl.Length++
}

func (fl *FileList) Get(key string) (*FileMeta, bool) {
	data, err := fl.DB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, false
		}
		log.Fatal(err)
	}
	result := fl.bytes2FileMetas(data)
	return result, true
}

func (fl *FileList) GetValue(key string) *FileMeta {
	data, err := fl.DB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil
		}
		log.Fatal(err)
	}
	result := fl.bytes2FileMetas(data)
	return result
}

func (fl *FileList) Delete(key string) {
	err := fl.DB.Delete([]byte(key), nil)
	if err != nil {
		log.Fatal(err)
	}
	fl.Length--
}

func (fl *FileList) Range() ([]string, []*FileMeta) {
	keys := []string{}
	values := []*FileMeta{}
	iter := fl.DB.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, string(key))
		value := iter.Value()
		values = append(values, fl.bytes2FileMetas(value))
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Fatal(err)
	}
	return keys, values
}

func (fl *FileList) fileMetas2Bytes(structs *FileMeta) []byte {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(structs)
	if err != nil {
		log.Fatal(err)
	}
	return b.Bytes()
}

func (fl *FileList) bytes2FileMetas(b []byte) *FileMeta {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	var data *FileMeta
	err := dec.Decode(&data)
	if err != nil {
		log.Fatal("Error decoding GOB data:", err)
	}
	return data
}

// =========================================================== DatanodeList ============================================================

func GetDatanodeList(dbPath string) *DatanodeList {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	iter := db.NewIterator(nil, nil)
	length := 0
	for iter.Next() {
		length++
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		log.Fatal(err)
	}
	return &DatanodeList{DB: db, Length: length}
}

func (dl *DatanodeList) GetLength() int {
	return dl.Length
}

func (dl *DatanodeList) Add(value *FileMeta) {
	valueBytes := dl.fileMetas2Bytes(value)
	key := dl.Length
	bytesKey := Int2Bytes(key)
	err := dl.DB.Put(bytesKey, valueBytes, nil)
	if err != nil {
		log.Fatal(err)
	}
	dl.Length++
}

func (dl *DatanodeList) Get(key int) (*FileMeta, bool) {
	var bytesKey []byte
	if key > dl.Length {
		log.Fatal(errors.New(fmt.Sprintf("out of index to DataNodeList: %v for length: %v", key, dl.Length)))
	} else {
		bytesKey = Int2Bytes(key)
	}
	data, err := dl.DB.Get(bytesKey, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, false
		}
		log.Fatal(err)
	}
	result := dl.bytes2FileMetas(data)
	return result, true
}

func (dl *DatanodeList) GetValue(key int) *FileMeta {
	var bytesKey []byte
	if key > dl.Length {
		log.Fatal(errors.New(fmt.Sprintf("out of index to DataNodeList: %v for length: %v", key, dl.Length)))
	} else {
		bytesKey = Int2Bytes(key)
	}
	data, err := dl.DB.Get(bytesKey, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil
		}
		log.Fatal(err)
	}
	result := dl.bytes2FileMetas(data)
	return result
}

func (dl *DatanodeList) Delete(key int) {
	DBlock.Lock()
	defer DBlock.Unlock()
	if key >= dl.Length {
		log.Fatal(errors.New(fmt.Sprintf("out of index to DataNodeList: %v for length: %v", key, dl.Length)))
	}
	for i := key; i < dl.Length-1; i++ {
		data, err := dl.DB.Get(Int2Bytes(i+1), nil)
		if err != nil {
			log.Fatal(err)
		}
		err = dl.DB.Put(Int2Bytes(i), data, nil)
		if err != nil {
			log.Fatal(err)
		}
	}
	err := dl.DB.Delete(Int2Bytes(dl.Length-1), nil)
	if err != nil {
		log.Fatal(err)
	}
	dl.Length--
}

func (dl *DatanodeList) Range() ([]int, []*FileMeta) {
	keys := []int{}
	values := []*FileMeta{}
	iter := dl.DB.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, Bytes2Int(key))
		value := iter.Value()
		values = append(values, dl.bytes2FileMetas(value))
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Fatal(err)
	}
	return keys, values
}

func (dl *DatanodeList) fileMetas2Bytes(structs *FileMeta) []byte {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(structs)
	if err != nil {
		log.Fatal(err)
	}
	return b.Bytes()
}

func (dl *DatanodeList) bytes2FileMetas(b []byte) *FileMeta {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	var data *FileMeta
	err := dec.Decode(&data)
	if err != nil {
		log.Fatal("Error decoding GOB data:", err)
	}
	return data
}

// Int2Bytes 整形转换成字节
func Int2Bytes(n int) []byte {
	x := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(bytesBuffer, binary.BigEndian, x)
	if err != nil {
		log.Fatal(err)
	}
	return bytesBuffer.Bytes()
}

// Bytes2Int 字节转换成整形
func Bytes2Int(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	err := binary.Read(bytesBuffer, binary.BigEndian, &x)
	if err != nil {
		log.Fatal(err)
	}
	return int(x)
}
