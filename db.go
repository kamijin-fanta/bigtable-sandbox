package main

import (
	"bytes"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Store interface {
	Get(key []byte) ([]byte, error)
	RangeGet(start, limit []byte) iterator.Iterator
	Put(key, value []byte) error
	RangeDelete(start, limit []byte) error
}

func hoge(rawKvClient tikv.RawKVClient) {
	//var s Store
	//s = &LeveldbStore{}
	//s = &TikvStore{}
}

type LeveldbStore struct {
	db *leveldb.DB
}

func (store *LeveldbStore) Get(key []byte) ([]byte, error) {
	res, err := store.db.Get(key, nil)
	if err != nil && err.Error() == "leveldb: not found" {
		return []byte{}, nil
	}
	return res, err
}

func (store *LeveldbStore) RangeGet(start, limit []byte) iterator.Iterator {
	return store.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
}

func (store *LeveldbStore) Put(key, value []byte) error {
	return store.db.Put(key, value, nil)
}

func (store *LeveldbStore) RangeDelete(start, limit []byte) error {
	iter := store.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	for iter.Next() {
		err := store.db.Delete(iter.Key(), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

type TikvStore struct {
	db *tikv.RawKVClient
}

func (store *TikvStore) Get(key []byte) ([]byte, error) {
	return store.db.Get(key)
}

func (store *TikvStore) RangeGet(start, limit []byte) iterator.Iterator {
	var iter iterator.Iterator
	iter = NewTikvIter(*store, start, limit, 1000)
	return iter
}

func (store *TikvStore) Put(key, value []byte) error {
	return store.db.Put(key, value)
}

func (store *TikvStore) RangeDelete(start, limit []byte) error {
	return store.db.DeleteRange(start, limit)
}

func NewTikvIter(store TikvStore, start, end []byte, windowLen int) *TikvIter {
	return &TikvIter{
		store: store,
		start: start,
		end:   end,
		limit: windowLen,
	}
}

type TikvIter struct {
	store TikvStore
	limit int
	start []byte
	end   []byte

	pos            int
	keyBuff        [][]byte
	valueBuff      [][]byte
	lastFetchedKey []byte

	released bool
	err      error
}

func (iter *TikvIter) First() bool {
	panic("implement me")
}

func (iter *TikvIter) Last() bool {
	panic("implement me")
}

func (iter *TikvIter) Seek(key []byte) bool {
	panic("implement me")
}

func (iter *TikvIter) Next() bool {
	if iter.released {
		iter.err = iterator.ErrIterReleased
		return false
	}
	if len(iter.keyBuff) == iter.pos {
		var start []byte

		if iter.lastFetchedKey == nil {
			start = iter.start
		} else {
			start = append(iter.lastFetchedKey, 0) // open bound
		}

		keys, values, err := iter.store.db.Scan(start, iter.limit)
		if err != nil {
			iter.err = err
			return false
		}
		iter.pos = -1
		iter.keyBuff = keys
		iter.valueBuff = values
		iter.lastFetchedKey = keys[len(keys)-1]
	}
	if len(iter.keyBuff) == 0 {
		return false
	}
	iter.pos += 1
	if bytes.Compare(iter.end, iter.Key()) <= 0 {
		return false
	}
	return true
}

func (iter *TikvIter) Prev() bool {
	panic("implement me")
}

func (iter *TikvIter) Release() {
	panic("implement me")
}

func (iter *TikvIter) SetReleaser(releaser util.Releaser) {
	panic("implement me")
}

func (iter *TikvIter) Valid() bool {
	panic("implement me")
}

func (iter *TikvIter) Error() error {
	panic("implement me")
}

func (iter *TikvIter) Key() []byte {
	return iter.keyBuff[iter.pos]
}

func (iter *TikvIter) Value() []byte {
	return iter.valueBuff[iter.pos]
}
