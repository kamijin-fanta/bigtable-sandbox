package services

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	bigtableAdmin "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"strings"
)

func MustNewServer(dbPath string) *grpc.Server {
	var store Store
	if strings.Index(dbPath, "pd://") == -1 {
		opts := &opt.Options{
			CompactionL0Trigger:           8,
			CompactionTableSize:           50 * 1024 * 1024,
			CompactionTotalSizeMultiplier: 10,
		}
		db, err := leveldb.OpenFile(dbPath, opts)
		if err != nil {
			panic(db)
		}
		store = &LeveldbStore{Db: db}
	} else {
		path := dbPath
		addressList := strings.Split(path[5:], ",")
		rawClient, err := rawkv.NewClient(addressList, config.Security{})
		if err != nil {
			panic(err)
		}
		fmt.Printf("tikv cluster: %v\n", rawClient.ClusterID())

		store = &TikvStore{
			Db: rawClient,
		}
	}
	service := MockBigtableService{Db: store}

	grpcServer := grpc.NewServer()
	bigtable.RegisterBigtableServer(grpcServer, &service)
	bigtableAdmin.RegisterBigtableTableAdminServer(grpcServer, &service)

	return grpcServer
}
