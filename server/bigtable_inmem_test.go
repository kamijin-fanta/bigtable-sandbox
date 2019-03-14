package main_test

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"context"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"testing"
)

func TestQuery(t *testing.T) {
	ctx := context.Background()
	var client *bigtable.Client
	var admin *bigtable.AdminClient
	{
		srv, err := bttest.NewServer("localhost:0")
		assert.Nil(t, err)
		conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
		assert.Nil(t, err)
		client, err = bigtable.NewClient(ctx, "", "", option.WithGRPCConn(conn))
		assert.Nil(t, err)
		admin, err = bigtable.NewAdminClient(ctx, "", "", option.WithGRPCConn(conn))
		assert.Nil(t, err)
	}

	t.Run("create table", func(t *testing.T) {
		ass := assert.New(t)
		err := admin.CreateTable(ctx, "example")
		ass.Nil(err)
		err = admin.CreateColumnFamily(ctx, "example", "cf1")
		ass.Nil(err)

		table := client.Open("example")

		row, err := table.ReadRow(ctx, "hoge")
		ass.Nil(err)
		ass.Nil(row)

		mut := bigtable.NewMutation()
		mut.Set("cf1", "fuga-col", bigtable.Now(), []byte("fuga-value"))
		err = table.Apply(ctx, "hoge", mut)
		ass.Nil(err)
		row, err = table.ReadRow(ctx, "hoge")
		ass.Nil(err)
		ass.Equal("fuga-value", string(row["cf1"][0].Value))
	})
}
