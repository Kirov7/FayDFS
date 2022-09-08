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

func (db *DB) GetDnSize() int {
	return db.Size
}

func (db *DB) Put(key string, value *FileMeta) {
	valueBytes := db.data2Bytes(value)
	err := db.DB.Put([]byte(key), valueBytes, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (db *DB) Get(key string) (*FileMeta, bool) {
	data, err := db.DB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, false
		}
		log.Fatal(err)
	}
	result := db.bytes2FileMetas(data)
	return result, true
}

func (db *DB) GetValue(key string) *FileMeta {
	data, err := db.DB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil
		}
		log.Fatal(err)
	}
	result := db.bytes2FileMetas(data)
	return result
}

func (db *DB) Delete(key string) {
	err := db.DB.Delete([]byte(key), nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (db *DB) Range() ([]string, []*FileMeta) {
	keys := []string{}
	values := []*FileMeta{}
	iter := db.DB.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, string(key))
		value := iter.Value()
		values = append(values, db.bytes2FileMetas(value))
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Fatal(err)
	}
	return keys, values
}

func (db *DB) UpdateDn(value map[string]*DatanodeMeta) {
	valueBytes := db.data2Bytes(value)
	err := db.DB.Put([]byte("dnList"), valueBytes, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (db *DB) AddDn(value map[string]*DatanodeMeta) {
	valueBytes := db.data2Bytes(value)
	err := db.DB.Put([]byte("dnList"), valueBytes, nil)
	if err != nil {
		log.Fatal(err)
	}
	db.Size++
}

func (db *DB) GetDn() map[string]*DatanodeMeta {
	data, err := db.DB.Get([]byte("dnList"), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil
		}
		log.Fatal(err)
	}
	result := db.bytes2DatanodeMetas(data)
	return result
}

func (db *DB) RaftGet(key []byte) ([]byte, error) {
	data, err := db.DB.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		log.Fatal(err)
	}
	return data, nil
}

func (db *DB) RaftPut(key []byte, state []byte) error {
	return db.DB.Put(key, state, nil)
}

func (db *DB) RaftDelete(key []byte) error {
	return db.DB.Delete(key, nil)
}

func (db *DB) RaftDelPrefixKeys(prefix string) error {
	iter := db.DB.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		err := db.DB.Delete(iter.Key(), nil)
		if err != nil {
			return err
		}
	}
	iter.Release()
	return nil
}

func (db *DB) data2Bytes(structs interface{}) []byte {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(structs)
	if err != nil {
		log.Fatal(err)
	}
	return b.Bytes()
}

func (db *DB) bytes2FileMetas(b []byte) *FileMeta {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	var data *FileMeta
	err := dec.Decode(&data)
	if err != nil {
		log.Fatal("Error decoding GOB data:", err)
	}
	return data
}

func (db *DB) bytes2DatanodeMetas(b []byte) map[string]*DatanodeMeta {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	var data map[string]*DatanodeMeta
	err := dec.Decode(&data)
	if err != nil {
		log.Fatal("Error decoding GOB data:", err)
	}
	return data
}
