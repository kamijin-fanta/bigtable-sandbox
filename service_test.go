package main

import (
	bigtableCli "cloud.google.com/go/bigtable"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	bigtableOption "google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/bigtable/admin/v2"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

func newClient() (conn *grpc.ClientConn, client bigtable.BigtableClient, ctx context.Context, closer func(), err error) {
	dbPath := "./test.db"
	listener := bufconn.Listen(1024 * 1024)

	db, err := leveldb.OpenFile(dbPath, nil)
	var store Store = &LeveldbStore{db: db}
	if err != nil {
		fmt.Printf("faild open %v", err)
		return nil, nil, nil, nil, err
	}

	grpcServer := grpc.NewServer()
	service := MockBigtableService{db: store}
	bigtable.RegisterBigtableServer(grpcServer, &service)
	admin.RegisterBigtableTableAdminServer(grpcServer, &service)

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
		return nil, nil, nil, nil, err
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
	key := KeyEncoder("TABLE", []byte("ROW:_KEY"), "FAMILY", []byte("COLUMN"))
	fmt.Printf("ASCII: %s / HEX: %x\n", key, key)

	key = KeyEncoder("TABLE", []byte("ROW:_KEY"), "FAMILY", []byte(""))
	fmt.Printf("ASCII: %s / HEX: %x\n", key, key)

	key = KeyEncoder("TABLE", []byte("ROW:_KEY"), "", []byte(""))
	fmt.Printf("ASCII: %s / HEX: %x\n", key, key)

	// Output:
	// ASCII: z:TABLE:ROW:_KEY:FAMILY:COLUMN / HEX: 7a3a5441424c453a524f573a5f4b45593a46414d494c593a434f4c554d4e
	// ASCII: z:TABLE:ROW:_KEY:FAMILY / HEX: 7a3a5441424c453a524f573a5f4b45593a46414d494c59
	// ASCII: z:TABLE:ROW:_KEY / HEX: 7a3a5441424c453a524f573a5f4b4559
}

func ExampleDecoder() {
	dst, _ := hex.DecodeString("7a3a5441424c453a524f573a5f4b45593a46414d494c593a434f4c554d4e")
	table, rowKey, family, colmn := KeyDecoder(dst)
	fmt.Printf("Table: %s / RowKey: %s / Family: %s / Column: %s", table, rowKey, family, colmn)
	// Output:
	// Table: TABLE / RowKey: ROW:_KEY / Family: FAMILY / Column: COLUMN
}

func TestService(t *testing.T) {
	conn, client, ctx, closer, err := newClient()
	if err != nil {
		t.Errorf("can not start client %v\n", err)
	}
	defer closer()

	t.Run("Mutate_SetValue", func(t *testing.T) {
		mut := &bigtable.Mutation{
			Mutation: &bigtable.Mutation_SetCell_{
				SetCell: &bigtable.Mutation_SetCell{
					FamilyName:      "default",
					ColumnQualifier: []byte("column"),
					Value:           []byte("test"),
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
		"example-project",
		"example-instance",
		bigtableOption.WithGRPCConn(conn),
	)
	assert.Nil(t, err, "user client connection error")
	if err != nil {
		panic(err)
	}

	btAdmClient, err := bigtableCli.NewAdminClient(
		ctx,
		"example-project",
		"example-instance",
		bigtableOption.WithGRPCConn(conn),
	)
	assert.Nil(t, err, "admin client connection error")
	if err != nil {
		panic(err)
	}

	t.Run("adminCli__create_table", func(t *testing.T) {
		ass := assert.New(t)
		err := btAdmClient.CreateTable(ctx, "default")
		ass.Nil(err)

		err = btAdmClient.CreateColumnFamily(ctx, "default", "cf1")
		ass.Nil(err)
		err = btAdmClient.CreateColumnFamily(ctx, "default", "cf2")
		ass.Nil(err)

		tableInfo, err := btAdmClient.TableInfo(ctx, "default")
		ass.Nil(err)
		ass.NotNil(tableInfo)
		ass.Len(tableInfo.Families, 2)

		err = btAdmClient.DeleteColumnFamily(ctx, "default", "cf2")
		ass.Nil(err)

		tableInfo, err = btAdmClient.TableInfo(ctx, "default")
		ass.Nil(err)
		ass.NotNil(tableInfo)
		ass.EqualValues([]string{"cf1"}, tableInfo.Families)
	})
	t.Run("cli__ReadRows", func(t *testing.T) {
		table := btClient.Open("default")
		row, err := table.ReadRow(ctx, "example_key")
		mut := bigtableCli.NewMutation()
		mut.DeleteRow()
		t.Logf("google read rows, %v %v", row, err)
	})

	t.Run("cli__e2e_1st", func(t *testing.T) {
		// Write -> Read -> Delete -> Read
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

	t.Run("cli__read_filters", func(t *testing.T) {
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

	t.Run("cli__range_read", func(t *testing.T) {
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

		r := bigtableCli.NewRange(rowKeyPrefix+"2", rowKeyPrefix+"5")
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
	t.Run("cli__mutate_rows", func(t *testing.T) {
		ass := assert.New(t)
		table := btClient.Open("default4")

		rowKeyPrefix := "range_k"

		writeMut := bigtableCli.NewMutation()
		writeMut.Set("cf", "c1", bigtableCli.Now(), []byte("v1"))
		errs, err := table.ApplyBulk(ctx, []string{
			rowKeyPrefix + "1",
			rowKeyPrefix + "2",
			rowKeyPrefix + "3",
			rowKeyPrefix + "4",
		}, []*bigtableCli.Mutation{
			writeMut,
			writeMut,
			writeMut,
			writeMut,
		})
		ass.Len(errs, 0)
		ass.Nil(err)

		r := bigtableCli.NewRange(rowKeyPrefix+"2", rowKeyPrefix+"5")
		all := make(bigtableCli.Row)
		err = table.ReadRows(ctx, r, func(row bigtableCli.Row) bool {
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
	t.Run("cli__heroic", func(t *testing.T) {
		ass := assert.New(t)
		table := btClient.Open("default5")

		rowKey := "\003\003foo\002\004\004host\017\017www.example.com\004\004site\003\003lon\000\000\001i\000\000\000\000"
		keyStop := "\003\003foo\002\004\004host\017\017www.example.com\004\004site\003\003lon\000\000\001i\000\000\000\001"

		writeMut := bigtableCli.NewMutation()
		writeMut.Set("points", "\x223\x374\x235\x200", bigtableCli.Now(), []byte("@E\x000\x000\x000\x000\x000\x000"))
		writeMut.Set("points", "\x224\x013\x337\x300", bigtableCli.Now(), []byte("@U\x000\x000\x000\x000\x000\x000"))
		err = table.Apply(ctx, rowKey, writeMut)
		ass.Nil(err)

		req := bigtable.ReadRowsRequest{
			TableName: "default5",
			Rows: &bigtable.RowSet{
				RowRanges: []*bigtable.RowRange{
					{
						StartKey: &bigtable.RowRange_StartKeyClosed{StartKeyClosed: []byte(rowKey)},
						EndKey:   &bigtable.RowRange_EndKeyOpen{EndKeyOpen: []byte(keyStop)},
					},
				},
			},
		}
		res, err := client.ReadRows(ctx, &req)
		ass.Nil(err)
		var all []*bigtable.ReadRowsResponse_CellChunk
		for {
			msg, err := res.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				t.Errorf("error: %+v", err)
				break
			}
			all = append(all, msg.Chunks...)
		}
		ass.Len(all, 2)
		//r := bigtableCli.NewRange(rowKey, keyStop)
		//all := make(bigtableCli.Row)
		//err = table.ReadRows(ctx, r, func(row bigtableCli.Row) bool {
		//	for k, v := range row {
		//		if all[k] == nil {
		//			all[k] = []bigtableCli.ReadItem{}
		//		}
		//		all[k] = append(all[k], v...)
		//	}
		//	return true
		//})
		//ass.Nil(err)
		//ass.Len(all["points"], 2)
	})
}
