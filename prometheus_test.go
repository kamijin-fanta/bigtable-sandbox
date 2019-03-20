package main

import (
	bigtableCli "cloud.google.com/go/bigtable"
	"fmt"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	bigtableOption "google.golang.org/api/option"
	"testing"
)

func TestPrometheus(t *testing.T) {
	ass := assert.New(t)
	conn, _, ctx, closer, err := newClient()
	if err != nil {
		t.Errorf("can not start client %v\n", err)
	}
	defer closer()

	remote, err := NewPrometheusRemote(conn, ctx, "example-project", "example-instance")

	ass.Nil(err)
	if err != nil {
		t.FailNow()
	}

	btCli, err := bigtableCli.NewClient(ctx, "example-project", "example-instance", bigtableOption.WithGRPCConn(conn))
	ass.Nil(err)
	if err != nil {
		t.FailNow()
	}

	t.Run("write", func(t *testing.T) {
		table := btCli.Open("prometheus")
		req := prompb.WriteRequest{
			Timeseries: []*prompb.TimeSeries{
				{
					Labels: []*prompb.Label{
						{
							Name:  "__name__",
							Value: "cpu_usage",
						},
						{
							Name:  "instance",
							Value: "192.168.11.1",
						},
						{
							Name:  "job",
							Value: "node_exporter",
						},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: 155307000000,
							Value:     1.5,
						},
						{
							Timestamp: 155307001000,
							Value:     1.6,
						},
						{
							Timestamp: 155307002000,
							Value:     1.7,
						},
						{
							Timestamp: 155307003000,
							Value:     1.8,
						},
					},
				},
			},
		}
		remote.RemoteWriter(req)
		r := bigtableCli.NewRange("metric:", "metric:"+string(255))
		var all []bigtableCli.Row
		err := table.ReadRows(ctx, r, func(rows bigtableCli.Row) bool {
			all = append(all, rows)
			return true
		})
		fmt.Printf("All: %v\n\n", all)
		ass.Nil(err)

		//matchers :=
		//CollectTargets(ctx, table, )
	})
}
