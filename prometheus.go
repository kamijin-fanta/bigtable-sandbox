package main

import (
	bigtableCli "cloud.google.com/go/bigtable"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	bigtableOption "google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

func NewPrometheusRemote(conn *grpc.ClientConn, ctx context.Context, project, instance string) (*PrometheusRemote, error) {
	client, err := bigtableCli.NewClient(
		context.Background(),
		project,
		instance,
		bigtableOption.WithGRPCConn(conn),
	)
	table := client.Open("prometheus")
	if err != nil {
		return nil, err
	}
	grpcClient := bigtable.NewBigtableClient(conn)
	return &PrometheusRemote{
		ctx:        ctx,
		conn:       conn,
		client:     client,
		table:      table,
		grpcClient: grpcClient,
	}, nil
}

type PrometheusRemote struct {
	ctx        context.Context
	conn       *grpc.ClientConn
	client     *bigtableCli.Client
	table      *bigtableCli.Table
	grpcClient bigtable.BigtableClient
}

func (p *PrometheusRemote) Register() {
	http.HandleFunc("/write", p.RemoteWrite)
	http.HandleFunc("/read", p.RemoteRead)
}

func (p *PrometheusRemote) RemoteWrite(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// resolve snappy
	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// resolve json
	var wreq prompb.WriteRequest

	if err := proto.Unmarshal(reqBuf, &wreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// resolve data
	err = p.RemoteWriter(wreq)
	if err != nil {
		fmt.Printf("RemoteWrite Error %v \n", err)
	}
	if _, err := w.Write([]byte("ok")); err != nil {
		return
	}
}

func canonicalId(labels []*prompb.Label) string {
	// example for: __name__=latency&host=192.168.11.1
	id := ""
	for i, label := range labels {
		if i != 0 {
			id += "&"
		}
		id += label.Name + "=" + label.Value
	}
	return id
}

func (p *PrometheusRemote) RemoteWriter(request prompb.WriteRequest) error {
	writeKeys := []string{}
	writeMutations := []*bigtableCli.Mutation{}
	for _, timeseries := range request.Timeseries {
		id := canonicalId(timeseries.Labels)
		for _, sample := range timeseries.Samples {
			// write metrics
			timeBuffer := make([]byte, 8)
			binary.BigEndian.PutUint64(timeBuffer, uint64(sample.Timestamp))

			valueArr := []byte(strconv.FormatFloat(sample.Value, 'E', -1, 64))

			mut := bigtableCli.NewMutation()
			mut.Set("met", "ts", bigtableCli.Now(), timeBuffer)
			mut.Set("met", "val", bigtableCli.Now(), valueArr)
			writeMutations = append(writeMutations, mut)

			key := "metric:" + id + ":" + string(timeBuffer)
			writeKeys = append(writeKeys, key)

			//fmt.Printf("Write Metrics %s key:%q\n", id, key)

			// build index
			indexKey := "index:" + id
			currentIndex, err := p.table.ReadRow(p.ctx, indexKey)
			if err != nil {
				return err
			}
			if len(currentIndex) == 0 {
				//fmt.Printf("Index not found\n")
				indexMut := bigtableCli.NewMutation()
				for _, label := range timeseries.Labels {
					// todo more intelligence
					indexMut.Set("index", label.Name, bigtableCli.Now(), []byte(label.Value))
				}
				err := p.table.Apply(p.ctx, indexKey, indexMut)
				if err != nil {
					return err
				}
			}
		}
	}

	errs, err := p.table.ApplyBulk(p.ctx, writeKeys, writeMutations)
	for i, e := range errs {
		if e != nil {
			fmt.Printf("BigTable write error %v. key: %v\n", e, writeKeys[i])
		}
	}
	if err != nil {
		fmt.Printf("BigTable write error %v\n", err)
		return err
	}
	return nil
}

func (p *PrometheusRemote) RemoteRead(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// snappy
	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// resolve json
	var rreq prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &rreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	naiveData, _ := p.RemoteReader(rreq)
	data, err := proto.Marshal(naiveData)
	if err != nil {
		fmt.Printf("Request is Failed %v\n", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	// sender
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	compressed = snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		return
	}
}

func ExtractLabel(key string) []*prompb.Label {
	segments := strings.Split(key, "&")
	res := make([]*prompb.Label, len(segments))
	for i, segment := range segments {
		terms := strings.Split(segment, "=")
		res[i] = &prompb.Label{
			Name:  terms[0],
			Value: terms[len(terms)-1],
		}
	}
	return res
}

func (p *PrometheusRemote) RemoteReader(request prompb.ReadRequest) (*prompb.ReadResponse, error) {
	var res prompb.ReadResponse
	for _, query := range request.Queries {
		fmt.Println(query)
		var queryResult prompb.QueryResult

		targets, _ := CollectTargets(p.ctx, p.grpcClient, query.Matchers)
		for _, target := range targets {
			var timeSeries prompb.TimeSeries
			timeSeries.Labels = ExtractLabel(target)

			startTimeBuff := make([]byte, 8)
			binary.BigEndian.PutUint64(startTimeBuff, uint64(query.StartTimestampMs))
			startKey := "metric:" + target + ":" + string(startTimeBuff)

			endTimeBuff := make([]byte, 8)
			binary.BigEndian.PutUint64(endTimeBuff, uint64(query.EndTimestampMs))
			endKey := "metric:" + target + ":" + string(endTimeBuff)

			r := bigtableCli.NewRange(startKey, endKey)
			err := p.table.ReadRows(p.ctx, r, func(rows bigtableCli.Row) bool {
				for _, row := range rows {
					sample := prompb.Sample{}
					for _, cell := range row {
						switch cell.Column {
						case "met:ts":
							sample.Timestamp = int64(binary.BigEndian.Uint64(cell.Value))
						case "met:val":
							val, _ := strconv.ParseFloat(string(cell.Value), 64)
							sample.Value = val
						default:
							fmt.Printf("Unkown column type %v\n", cell)
						}
					}
					timeSeries.Samples = append(timeSeries.Samples, sample)
				}

				return true
			})
			if err != nil {
				return nil, err
			}

			queryResult.Timeseries = append(queryResult.Timeseries, &timeSeries)
		}

		res.Results = append(res.Results, &queryResult)
	}
	return &res, nil
}
func CollectTargets(ctx context.Context, client bigtable.BigtableClient, matchers []*prompb.LabelMatcher) ([]string, error) {
	var subFilters []*bigtable.RowFilter
	// todo support to NotEqual, RegexEqual, NotRegexEqual
	for _, matcher := range matchers {
		f := &bigtable.RowFilter{
			Filter: &bigtable.RowFilter_Chain_{
				Chain: &bigtable.RowFilter_Chain{
					Filters: []*bigtable.RowFilter{
						{
							Filter: &bigtable.RowFilter_ColumnQualifierRegexFilter{
								ColumnQualifierRegexFilter: []byte(matcher.Name),
							},
						},
						{
							Filter: &bigtable.RowFilter_ValueRegexFilter{
								ValueRegexFilter: []byte(matcher.Value),
							},
						},
					},
				},
			},
		}
		subFilters = append(subFilters, f)
	}
	var targets []string

	fetchOffset := 0
	if len(subFilters) > 0 {
		fetchOffset = len(subFilters) - 1
	}

	req := &bigtable.ReadRowsRequest{
		TableName: "prometheus",
		Rows: &bigtable.RowSet{
			RowRanges: []*bigtable.RowRange{
				{
					StartKey: &bigtable.RowRange_StartKeyClosed{[]byte("index:")},
					EndKey:   &bigtable.RowRange_EndKeyOpen{append([]byte("index:"), 255)},
				},
			},
		},
		Filter: &bigtable.RowFilter{
			Filter: &bigtable.RowFilter_Condition_{ // セルが含まれる場合は、PassAllしたい
				Condition: &bigtable.RowFilter_Condition{
					PredicateFilter: &bigtable.RowFilter{
						Filter: &bigtable.RowFilter_Chain_{ // セルの条件がOKなら、OffsetPerRowを行う
							Chain: &bigtable.RowFilter_Chain{
								Filters: []*bigtable.RowFilter{
									{
										Filter: &bigtable.RowFilter_Interleave_{ // ヒットするセルの数を数える
											Interleave: &bigtable.RowFilter_Interleave{
												Filters: subFilters,
											},
										},
									},
									{
										// 全てのSubFiltersにマッチした場合のみ、1セルだけ出力する
										Filter: &bigtable.RowFilter_CellsPerRowOffsetFilter{int32(fetchOffset)},
									},
								},
							},
						},
					},
					TrueFilter: &bigtable.RowFilter{ // マッチするセルを、行につき1つだけ出力する
						Filter: &bigtable.RowFilter_CellsPerRowLimitFilter{1},
					},
				},
			},
		},
	}

	res, err := client.ReadRows(ctx, req)
	for {
		msg, err := res.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		for _, chunk := range msg.Chunks {
			keyArr := strings.Split(string(chunk.RowKey), ":")
			targets = append(targets, keyArr[len(keyArr)-1])
			//fmt.Printf("TARGETSSSSSSSSSSS: %s / %s / %q\n", chunk.RowKey, chunk.FamilyName.Value, chunk.Value)
		}
	}

	if err != nil {
		return nil, err
	}

	return targets, nil
}
