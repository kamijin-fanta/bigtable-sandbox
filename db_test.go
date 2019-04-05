package main

import (
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"os"
	"strings"
	"testing"
)

func init() {
	godotenv.Load()
}

func TestLevedbStore(t *testing.T) {
	ass := assert.New(t)

	dbPath := "./test.store"
	db, err := leveldb.OpenFile(dbPath, nil)
	ass.Nil(err)
	if err != nil {
		os.Exit(1)
	}

	var store Store = &LeveldbStore{
		db: db,
	}

	StoreSpecs(t, store)

	db.Close()
	os.RemoveAll(dbPath)
}

func TestTikvStore(t *testing.T) {
	ass := assert.New(t)

	pdAddress := os.Getenv("PD_ADDRESS")
	addressList := strings.Split(pdAddress, ",")
	rawClient, err := rawkv.NewClient(addressList, config.Security{})
	ass.Nil(err)
	if err != nil {
		os.Exit(1)
	}

	var store Store
	store = &TikvStore{
		db: rawClient,
	}

	StoreSpecs(t, store)

	rawClient.Close()
}

func StoreSpecs(t *testing.T, store Store) {
	ass := assert.New(t)

	t.Run("read write", func(t *testing.T) {
		err := store.Put([]byte("z:example1:hoge-key"), []byte("content"))
		ass.Nil(err)
		res, err := store.Get([]byte("z:example1:hoge-key"))
		ass.Equal([]byte("content"), res)
	})
	t.Run("range read", func(t *testing.T) {
		err := store.BatchPut(
			[][]byte{
				[]byte("z:example2:1"),
				[]byte("z:example2:2"),
				[]byte("z:example2:3"),
				[]byte("z:example2:4"),
			},
			[][]byte{
				[]byte("1st"),
				[]byte("2nd"),
				[]byte("3rd"),
				[]byte("4th"),
			},
		)
		ass.Nil(err)

		iter := store.RangeGet([]byte("z:example2:1"), []byte("z:example2:4"))
		ass.True(iter.Next())
		ass.Equal([]byte("z:example2:1"), iter.Key())
		ass.Equal([]byte("1st"), iter.Value())
		ass.True(iter.Next())
		ass.Equal([]byte("z:example2:2"), iter.Key())
		ass.Equal([]byte("2nd"), iter.Value())
		ass.True(iter.Next())
		ass.Equal([]byte("z:example2:3"), iter.Key())
		ass.Equal([]byte("3rd"), iter.Value())
		ass.False(iter.Next())
	})
	t.Run("celan", func(t *testing.T) {
		err := store.RangeDelete([]byte("z:"), []byte("z:z"))
		ass.Nil(err)
		res, err := store.Get([]byte("z:example1:hoge-key"))
		ass.Nil(err)
		ass.Len(res, 0)
	})
}
