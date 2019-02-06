package main

import (
	"context"
	"fmt"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"io"
)

func main() {
	conn, err := grpc.Dial("127.0.0.1:8111", grpc.WithInsecure())
	if err != nil {
		panic("can not connect")
	}
	defer conn.Close()

	client := bigtable.NewBigtableClient(conn)
	req := bigtable.ReadRowsRequest{
		Rows: &bigtable.RowSet{
			RowKeys: nil,
		},
	}
	res, err := client.ReadRows(context.Background(), &req)
	if err != nil {
		fmt.Printf("error: %+v", err)
		panic("errr")
	}
	for {
		msg, err := res.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Printf("error: %+v", err)
			break
		}
		fmt.Printf("res: %+v", msg)
	}
}
