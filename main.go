package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	bigtableAdmin "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"net/http"
)

func main() {
	lis, err := net.Listen("tcp", ":8111")
	if err != nil {
		panic("failed to listen")
	}

	dbPath := flag.String("db", "./data.db", "db path")
	db, err := leveldb.OpenFile(*dbPath, nil)
	if err != nil {
		panic(db)
	}

	var store Store = &LeveldbStore{db: db}
	service := MockBigtableService{db: store}

	grpcServer := grpc.NewServer()
	bigtable.RegisterBigtableServer(grpcServer, &service)
	bigtableAdmin.RegisterBigtableTableAdminServer(grpcServer, &service)

	stop := make(chan bool)

	go func() {
		fmt.Println("start grpc server in :8111")
		err = grpcServer.Serve(lis)
		if err != nil {
			panic("can not start")
		}
		stop <- true
	}()

	listener := bufconn.Listen(5 * 1024 * 1024)
	go func() {
		err = grpcServer.Serve(listener)
		if err != nil {
			panic("can not start bufconn")
		}
	}()

	go func() {
		ctx := context.Background()
		conn, err := grpc.DialContext(
			ctx,
			"bufnet",
			grpc.WithContextDialer(func(ctx context.Context, addr string) (conn net.Conn, e error) {
				return listener.Dial()
			}),
			grpc.WithInsecure(),
		)
		remote, err := NewPrometheusRemote(conn, ctx, "project", "instance")
		if err != nil {
			panic("can not connect to remote")
		}
		remote.Register()
		fmt.Println("start http server in :8112")
		err = http.ListenAndServe(":8112", nil)
		if err != nil {
			panic("can not start http prometheus server")
		}
		stop <- true
	}()
	<-stop
}
