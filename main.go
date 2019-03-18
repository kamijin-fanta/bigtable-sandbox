package main

import (
	"fmt"
	bigtableAdmin "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"net"
)

func main() {
	lis, err := net.Listen("tcp", "127.0.0.1:8111")
	if err != nil {
		panic("failed to listen")
	}
	service := MockBigtableService{}
	grpcServer := grpc.NewServer()
	bigtable.RegisterBigtableServer(grpcServer, &service)
	bigtableAdmin.RegisterBigtableTableAdminServer(grpcServer, &service)
	fmt.Println("start :8111")
	err = grpcServer.Serve(lis)
	if err != nil {
		panic("can not start")
	}
}
