package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/kamijin-fanta/bigtable-sandbox/services"
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

	dbPath := flag.String("store", "./data.store", "store path")
	flag.Parse()
	fmt.Printf("use store: %s\n", *dbPath)
	grpcServer := services.MustNewServer(*dbPath)

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
		remote, err := services.NewPrometheusRemote(conn, ctx, "project", "instance")
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
