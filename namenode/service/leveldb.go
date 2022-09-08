package service

import (
	"bytes"
	"encoding/gob"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"log"
)

type DB struct {
	Size int
	DB   *leveldb.DB
}

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

func (fm *DB) RaftGet(key []byte) ([]byte, error) {
	data, err := fm.DB.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		log.Fatal(err)
	}
	return data, nil
}

func (fm *DB) RaftPut(key []byte, state []byte) error {
	return fm.DB.Put(key, state, nil)
}

func (fm *DB) RaftDelete(key []byte) error {
	return fm.DB.Delete(key, nil)
}

func (fm *DB) RaftDelPrefixKeys(prefix string) error {
	iter := fm.DB.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		err := fm.DB.Delete(iter.Key(), nil)
		if err != nil {
			return err
		}
	}
	iter.Release()
	return nil
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
