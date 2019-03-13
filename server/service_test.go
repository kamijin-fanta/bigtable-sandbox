package main

import (
	bigtableCli "cloud.google.com/go/bigtable"
	"context"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	bigtableOption "google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"io"
	"net"
	"testing"
	"time"
)

func newClient() (conn *grpc.ClientConn, client bigtable.BigtableClient, ctx context.Context) {
	listener := bufconn.Listen(1024 * 1024)

	db, err := leveldb.OpenFile("./test.db", nil)

	grpcServer := grpc.NewServer()
	service := MockBigtableService{db}
	bigtable.RegisterBigtableServer(grpcServer, &service)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			fmt.Printf("Server exites with error: %v", err)
			panic("failed start to server")
		}
	}()

	ctx = context.Background()
	conn, err = grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithInsecure(),
	)
	if err != nil {
		fmt.Printf("failed dial %v", err)
		panic("failed start to server")
	}
	client = bigtable.NewBigtableClient(conn)
	return
}

func TestService(t *testing.T) {
	conn, client, ctx := newClient()
	defer conn.Close()

	t.Run("Mutate Set Value", func(t *testing.T) {
		mut := &bigtable.Mutation{
			Mutation: &bigtable.Mutation_SetCell_{
				SetCell: &bigtable.Mutation_SetCell{
					FamilyName: "default",
					Value:      []byte("test"),
				},
			},
		}
		_, err := client.MutateRow(ctx, &bigtable.MutateRowRequest{
			TableName: "default",
			RowKey:    []byte("example_key"),
			Mutations: []*bigtable.Mutation{mut},
		})
		if err != nil {
			t.Errorf("failed write rows %v", err)
		}
	})
	t.Run("ReadRows", func(t *testing.T) {
		req := bigtable.ReadRowsRequest{
			TableName: "default",
			Rows: &bigtable.RowSet{
				RowKeys: [][]byte{
					[]byte("example_key"),
				},
			},
		}
		res, err := client.ReadRows(ctx, &req)
		if err != nil {
			t.Errorf("failed read rows %v", err)
		}

		for {
			msg, err := res.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				t.Errorf("error: %+v", err)
				break
			}
			t.Logf("res: %+v", msg)
		}
	})

	btClient, err := bigtableCli.NewClient(
		ctx,
		"",
		"",
		bigtableOption.WithGRPCConn(conn),
	)
	if err != nil {
		t.Errorf("bigtable client connection error %v", err)
	}
	t.Run("google ReadRows", func(t *testing.T) {
		table := btClient.Open("default")
		row, err := table.ReadRow(ctx, "example_key")
		t.Logf("google read rows, %v %v", row, err)
	})
}
