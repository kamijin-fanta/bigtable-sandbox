package main

import (
	bigtableCli "cloud.google.com/go/bigtable"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	bigtableOption "google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/bigtable/v2"
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

	btClient := bigtable.NewBigtableClient(conn)
	table := btCli.Open("prometheus")
	t.Run("write", func(t *testing.T) {
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
				{
					Labels: []*prompb.Label{
						{
							Name:  "__name__",
							Value: "cpu_usage",
						},
						{
							Name:  "instance",
							Value: "172.24.1.1",
						},
						{
							Name:  "job",
							Value: "node_exporter",
						},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: 155307000000,
							Value:     2.5,
						},
						{
							Timestamp: 155307001000,
							Value:     2.6,
						},
						{
							Timestamp: 155307002000,
							Value:     2.7,
						},
						{
							Timestamp: 155307003000,
							Value:     2.8,
						},
					},
				},
				{
					Labels: []*prompb.Label{
						{
							Name:  "__name__",
							Value: "memory_available",
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
							Value:     10000000000,
						},
						{
							Timestamp: 155307001000,
							Value:     11000000000,
						},
						{
							Timestamp: 155307002000,
							Value:     12000000000,
						},
						{
							Timestamp: 155307003000,
							Value:     13000000000,
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
		ass.Nil(err)
		ass.Len(all, 12)
		ass.Equal("met:ts", all[0]["met"][0].Column)
		ass.Equal("met:val", all[0]["met"][1].Column)
	})
	t.Run("CollectTargets", func(t *testing.T) {
		{
			matchers := []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "cpu_usage",
				},
			}
			targets, err := CollectTargets(ctx, btClient, matchers)
			ass.Nil(err)
			ass.EqualValues(
				[]string{
					"__name__=cpu_usage&instance=172.24.1.1&job=node_exporter",
					"__name__=cpu_usage&instance=192.168.11.1&job=node_exporter",
				},
				targets,
			)
		}
		{
			matchers := []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "cpu_usage",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "instance",
					Value: "192.168.11.1",
				},
			}
			targets, err := CollectTargets(ctx, btClient, matchers)
			ass.Nil(err)
			ass.EqualValues(
				[]string{
					"__name__=cpu_usage&instance=192.168.11.1&job=node_exporter",
				},
				targets,
			)
		}

		//t.Logf("Logf %v\n", targets)
	})
}
