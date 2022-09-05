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

type DB struct {
	Size int
	DB   *leveldb.DB
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

func (dl *DatanodeList) Add(value *DatanodeMeta) {
	valueBytes := dl.datanodeMetas2Bytes(value)
	key := dl.Length
	bytesKey := Int2Bytes(key)
	err := dl.DB.Put(bytesKey, valueBytes, nil)
	if err != nil {
		log.Fatal(err)
	}
	dl.Length++
}

func (dl *DatanodeList) Update(key int, value *DatanodeMeta) {
	valueBytes := dl.datanodeMetas2Bytes(value)
	bytesKey := Int2Bytes(key)
	if exit, _ := dl.DB.Has(bytesKey, nil); !exit {
		log.Fatal("dataNode is not exit, can not update dataNode")
	}
	err := dl.DB.Put(bytesKey, valueBytes, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (dl *DatanodeList) Get(key int) (*DatanodeMeta, bool) {
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
	result := dl.bytes2DatanodeMetas(data)
	return result, true
}

func (dl *DatanodeList) GetValue(key int) *DatanodeMeta {
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
	result := dl.bytes2DatanodeMetas(data)
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

func (dl *DatanodeList) HasValue(ipAddr string) int {
	iter := dl.DB.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		if dl.bytes2DatanodeMetas(value).IPAddr == ipAddr {
			return Bytes2Int(key)
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Fatal(err)
	}
	return -1
}

func (dl *DatanodeList) Range() ([]int, []*DatanodeMeta) {
	keys := []int{}
	values := []*DatanodeMeta{}
	iter := dl.DB.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, Bytes2Int(key))
		value := iter.Value()
		values = append(values, dl.bytes2DatanodeMetas(value))
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Fatal(err)
	}
	return keys, values
}

func (dl *DatanodeList) datanodeMetas2Bytes(structs *DatanodeMeta) []byte {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(structs)
	if err != nil {
		log.Fatal(err)
	}
	return b.Bytes()
}

func (dl *DatanodeList) bytes2DatanodeMetas(b []byte) *DatanodeMeta {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	var data *DatanodeMeta
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

// =========================================================== FileMetas ============================================================

func GetDB(dbPath string) *DB {
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
	return &DB{DB: db, Size: length}
}

func (fm *DB) GetDnSize() int {
	return fm.Size
}

func (fm *DB) Put(key string, value *FileMeta) {
	valueBytes := fm.data2Bytes(value)
	err := fm.DB.Put([]byte(key), valueBytes, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (fm *DB) Get(key string) (*FileMeta, bool) {
	data, err := fm.DB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, false
		}
		log.Fatal(err)
	}
	result := fm.bytes2FileMetas(data)
	return result, true
}

func (fm *DB) GetValue(key string) *FileMeta {
	data, err := fm.DB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil
		}
		log.Fatal(err)
	}
	result := fm.bytes2FileMetas(data)
	return result
}

func (fm *DB) Delete(key string) {
	err := fm.DB.Delete([]byte(key), nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (fm *DB) Range() ([]string, []*FileMeta) {
	keys := []string{}
	values := []*FileMeta{}
	iter := fm.DB.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, string(key))
		value := iter.Value()
		values = append(values, fm.bytes2FileMetas(value))
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Fatal(err)
	}
	return keys, values
}

func (fm *DB) UpdateDn(value map[string]*DatanodeMeta) {
	valueBytes := fm.data2Bytes(value)
	err := fm.DB.Put([]byte("dnList"), valueBytes, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (fm *DB) AddDn(value map[string]*DatanodeMeta) {
	valueBytes := fm.data2Bytes(value)
	err := fm.DB.Put([]byte("dnList"), valueBytes, nil)
	if err != nil {
		log.Fatal(err)
	}
	fm.Size++
}

func (fm *DB) GetDn() map[string]*DatanodeMeta {
	data, err := fm.DB.Get([]byte("dnList"), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil
		}
		log.Fatal(err)
	}
	result := fm.bytes2DatanodeMetas(data)
	return result
}

func (fm *DB) data2Bytes(structs interface{}) []byte {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(structs)
	if err != nil {
		log.Fatal(err)
	}
	return b.Bytes()
}

func (fm *DB) bytes2FileMetas(b []byte) *FileMeta {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	var data *FileMeta
	err := dec.Decode(&data)
	if err != nil {
		log.Fatal("Error decoding GOB data:", err)
	}
	return data
}

func (fm *DB) bytes2DatanodeMetas(b []byte) map[string]*DatanodeMeta {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	var data map[string]*DatanodeMeta
	err := dec.Decode(&data)
	if err != nil {
		log.Fatal("Error decoding GOB data:", err)
	}
	return data
}
