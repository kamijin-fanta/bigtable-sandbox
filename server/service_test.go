package main

import (
	bigtableCli "cloud.google.com/go/bigtable"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	bigtableOption "google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

func newClient() (conn *grpc.ClientConn, client bigtable.BigtableClient, ctx context.Context, closer func()) {
	dbPath := "./test.db"
	listener := bufconn.Listen(1024 * 1024)

	db, err := leveldb.OpenFile(dbPath, nil)

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
	closer = func() {
		conn.Close()
		db.Close()
		os.RemoveAll(dbPath)
	}
	return
}

func ExampleEncoder() {
	key := KeyEncoder([]byte("ROW_KEY"), "FAMILY", []byte("COLUMN"))
	fmt.Printf("ASCII: %s / HEX: %x\n", key, key)

	key = KeyEncoder([]byte("ROW_KEY"), "FAMILY", []byte(""))
	fmt.Printf("ASCII: %s / HEX: %x\n", key, key)

	key = KeyEncoder([]byte("ROW_KEY"), "", []byte(""))
	fmt.Printf("ASCII: %s / HEX: %x\n", key, key)

	// Output:
	// ASCII: z:ROW_KEY:FAMILY:COLUMN / HEX: 7a3a524f575f4b45593a46414d494c593a434f4c554d4e
	// ASCII: z:ROW_KEY:FAMILY / HEX: 7a3a524f575f4b45593a46414d494c59
	// ASCII: z:ROW_KEY / HEX: 7a3a524f575f4b4559
}

func ExampleDecoder() {
	dst, _ := hex.DecodeString("7a3a524f575f4b45593a46414d494c593a434f4c554d4e")
	rowKey, family, colmn := KeyDecoder(dst)
	fmt.Printf("RowKey: %s / Family: %s / Column: %s", rowKey, family, colmn)
	// Output:
	// RowKey: ROW_KEY / Family: FAMILY / Column: COLUMN
}

func TestService(t *testing.T) {
	conn, client, ctx, closer := newClient()
	defer closer()

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
	t.Run("google cli / ReadRows", func(t *testing.T) {
		table := btClient.Open("default")
		row, err := table.ReadRow(ctx, "example_key")
		mut := bigtableCli.NewMutation()
		mut.DeleteRow()
		t.Logf("google read rows, %v %v", row, err)
	})

	t.Run("google cli / Write -> Read -> Delete -> Read", func(t *testing.T) {
		ass := assert.New(t)
		table := btClient.Open("default")

		rowKey := "key_name"

		writeMut := bigtableCli.NewMutation()
		writeMut.Set("default", "hoge", bigtableCli.Now(), []byte("fugafuga"))
		err = table.Apply(ctx, rowKey, writeMut)
		ass.Nil(err)

		row, err := table.ReadRow(ctx, rowKey)
		ass.Nil(err)
		ass.Equal(rowKey, row.Key())
		ass.Equal(1, len(row["default"]))
		ass.Equal(rowKey, row["default"][0].Row)
		ass.Equal("fugafuga", string(row["default"][0].Value))

		delMut := bigtableCli.NewMutation()
		delMut.DeleteCellsInFamily("default")
		err = table.Apply(ctx, rowKey, delMut)
		ass.Nil(err)

		row, err = table.ReadRow(ctx, rowKey)
		ass.Nil(err)
		ass.Equal(0, len(row["default"]))
	})
}
