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
	"google.golang.org/grpc"
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
	return &PrometheusRemote{
		ctx:    ctx,
		conn:   conn,
		client: client,
		table:  table,
	}, nil
}

type PrometheusRemote struct {
	ctx    context.Context
	conn   *grpc.ClientConn
	client *bigtableCli.Client
	table  *bigtableCli.Table
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
			mut.Set("met", "ts", bigtableCli.Now(), valueArr)
			mut.Set("met", "val", bigtableCli.Now(), timeBuffer)
			writeMutations = append(writeMutations, mut)

			key := "metric:" + id + string(timeBuffer)
			writeKeys = append(writeKeys, key)

			fmt.Printf("Write Metrics %v %v\n", key, mut)

			// build index
			indexKey := "index:" + id
			currentIndex, err := p.table.ReadRow(p.ctx, indexKey)
			if err != nil {
				return err
			}
			if len(currentIndex) == 0 {
				fmt.Printf("Index not found\n")
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
		fmt.Println(timeseries)
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
	naiveData := p.RemoteReader(rreq)
	data, _ := proto.Marshal(naiveData)
	// sender
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	compressed = snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		return
	}
}

func (p *PrometheusRemote) RemoteReader(request prompb.ReadRequest) *prompb.ReadResponse {
	for _, query := range request.Queries {
		CollectTargets(p.ctx, p.table, query.Matchers)
		fmt.Println(query)
	}
	return nil
}
func CollectTargets(ctx context.Context, table *bigtableCli.Table, matchers []*prompb.LabelMatcher) ([]string, error) {
	var filters []bigtableCli.Filter
	// todo support to NotEqual, RegexEqual, NotRegexEqual
	for _, matcher := range matchers {
		nameFilter := bigtableCli.ColumnFilter(matcher.Name)
		valueFilter := bigtableCli.ValueFilter(matcher.Value)
		filters = append(filters, bigtableCli.ChainFilters(nameFilter, valueFilter))
	}
	filter := bigtableCli.RowFilter(bigtableCli.ChainFilters(filters...))
	readRange := bigtableCli.NewRange("index:", "index:"+string(255))

	var targets []string

	err := table.ReadRows(ctx, readRange, func(rows bigtableCli.Row) bool {
		ind := rows["index"]
		if ind != nil {
			for _, item := range ind {
				keySplit := strings.Split(item.Row, ":")
				target := keySplit[len(keySplit)-1]
				targets = append(targets, target)
			}
		}
		return true
	}, filter)

	if err != nil {
		return nil, err
	}

	return targets, nil
}
