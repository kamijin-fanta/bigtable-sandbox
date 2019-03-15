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
	key := KeyEncoder("TABLE", []byte("ROW_KEY"), "FAMILY", []byte("COLUMN"))
	fmt.Printf("ASCII: %s / HEX: %x\n", key, key)

	key = KeyEncoder("TABLE", []byte("ROW_KEY"), "FAMILY", []byte(""))
	fmt.Printf("ASCII: %s / HEX: %x\n", key, key)

	key = KeyEncoder("TABLE", []byte("ROW_KEY"), "", []byte(""))
	fmt.Printf("ASCII: %s / HEX: %x\n", key, key)

	// Output:
	// ASCII: z:TABLE:ROW_KEY:FAMILY:COLUMN / HEX: 7a3a5441424c453a524f575f4b45593a46414d494c593a434f4c554d4e
	// ASCII: z:TABLE:ROW_KEY:FAMILY / HEX: 7a3a5441424c453a524f575f4b45593a46414d494c59
	// ASCII: z:TABLE:ROW_KEY / HEX: 7a3a5441424c453a524f575f4b4559
}

func ExampleDecoder() {
	dst, _ := hex.DecodeString("7a3a5441424c453a524f575f4b45593a46414d494c593a434f4c554d4e")
	table, rowKey, family, colmn := KeyDecoder(dst)
	fmt.Printf("Table: %s / RowKey: %s / Family: %s / Column: %s", table, rowKey, family, colmn)
	// Output:
	// Table: TABLE / RowKey: ROW_KEY / Family: FAMILY / Column: COLUMN
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

	t.Run("google cli / read filters", func(t *testing.T) {
		ass := assert.New(t)
		table := btClient.Open("default2")

		rowKeyPrefix := "filters_k"

		writeMut := bigtableCli.NewMutation()
		writeMut.Set("cfA", "cA1", bigtableCli.Now(), []byte("vA1"))
		writeMut.Set("cfA", "cA2", bigtableCli.Now(), []byte("vA2"))
		writeMut.Set("cfA", "cA3", bigtableCli.Now(), []byte("vA3"))
		writeMut.Set("cfA", "cA4", bigtableCli.Now(), []byte("vA4"))
		writeMut.Set("cfB", "cB1", bigtableCli.Now(), []byte("vB1"))
		writeMut.Set("cfB", "cB2", bigtableCli.Now(), []byte("vB2"))
		writeMut.Set("cfC", "cC1", bigtableCli.Now(), []byte("vC1"))
		err = table.Apply(ctx, rowKeyPrefix+"1", writeMut)
		ass.Nil(err)

		filter := bigtableCli.RowFilter(
			bigtableCli.InterleaveFilters(
				bigtableCli.ChainFilters(
					bigtableCli.FamilyFilter("cfA"),
					bigtableCli.ValueRangeFilter([]byte("vA1"), []byte("vA3")),
				),
				bigtableCli.ColumnFilter("cB."),
			),
		)

		row, err := table.ReadRow(ctx, rowKeyPrefix+"1", filter)
		ass.Nil(err)
		//for cf := range row {
		//	for i, cell := range row[cf] {
		//		t.Logf("=>>> %s %d %+v %s\n", cf, i, cell, cell.Value)
		//	}
		//}

		ass.Len(row["cfA"], 2)
		ass.Equal("vA2", string(row["cfA"][0].Value))
		ass.Equal("vA3", string(row["cfA"][1].Value))
		ass.Len(row["cfB"], 2)
		ass.Equal("vB1", string(row["cfB"][0].Value))
		ass.Equal("vB2", string(row["cfB"][1].Value))
		ass.Len(row["cfC"], 0)
	})

	t.Run("google cli / range read", func(t *testing.T) {
		ass := assert.New(t)
		table := btClient.Open("default3")

		rowKeyPrefix := "range_k"

		writeMut := bigtableCli.NewMutation()
		writeMut.Set("cf", "c1", bigtableCli.Now(), []byte("v1"))
		err = table.Apply(ctx, rowKeyPrefix+"1", writeMut)
		err = table.Apply(ctx, rowKeyPrefix+"2", writeMut)
		err = table.Apply(ctx, rowKeyPrefix+"3", writeMut)
		err = table.Apply(ctx, rowKeyPrefix+"4", writeMut)
		ass.Nil(err)

		r := bigtableCli.NewRange(rowKeyPrefix+"1", rowKeyPrefix+"4")
		all := make(bigtableCli.Row)
		err := table.ReadRows(ctx, r, func(row bigtableCli.Row) bool {
			for k, v := range row {
				if all[k] == nil {
					all[k] = []bigtableCli.ReadItem{}
				}
				all[k] = append(all[k], v...)
			}
			return true
		})
		ass.Nil(err)
		ass.Len(all["cf"], 3)
		ass.Equal(rowKeyPrefix+"2", all["cf"][0].Row)
		ass.Equal(rowKeyPrefix+"3", all["cf"][1].Row)
		ass.Equal(rowKeyPrefix+"4", all["cf"][2].Row)
	})
}
