package main

import (
	"fmt"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"net"
)

func main() {
	lis, err := net.Listen("tcp", "127.0.0.1:8111")
	if err != nil {
		panic("failed to listen")
	}
	service := MockBigtableService{}
	grpcServer := grpc.NewServer()
	bigtable.RegisterBigtableServer(grpcServer, &service)
	fmt.Println("start :8111")
	err = grpcServer.Serve(lis)
	if err != nil {
		panic("can not start")
	}
}

type MockBigtableService struct {
	db *leveldb.DB
}

func (service *MockBigtableService) ReadRows(req *bigtable.ReadRowsRequest, server bigtable.Bigtable_ReadRowsServer) error {
	var chunks []*bigtable.ReadRowsResponse_CellChunk
	for _, key := range req.Rows.RowKeys {
		value, err := service.db.Get(key, nil)
		if err != nil {
			return err
		}
		chunks = append(chunks, &bigtable.ReadRowsResponse_CellChunk{
			RowKey:          key,
			Value:           value,
			FamilyName:      &wrappers.StringValue{Value: "default"},
			Qualifier:       &wrappers.BytesValue{Value: key},
			TimestampMicros: 0,
			Labels:          []string{"label"},
		})
	}
	if len(chunks) > 0 {
		chunks[len(chunks)-1].RowStatus = &bigtable.ReadRowsResponse_CellChunk_CommitRow{true}
	}
	res := bigtable.ReadRowsResponse{
		LastScannedRowKey: []byte("fuga"),
		Chunks:            chunks,
	}
	server.Send(&res)
	return nil
}

func (MockBigtableService) SampleRowKeys(*bigtable.SampleRowKeysRequest, bigtable.Bigtable_SampleRowKeysServer) error {
	panic("implement me")
}

func (service *MockBigtableService) MutateRow(ctx context.Context, req *bigtable.MutateRowRequest) (*bigtable.MutateRowResponse, error) {
	for i := range req.Mutations {
		switch m := req.Mutations[i].Mutation.(type) {
		case *bigtable.Mutation_SetCell_:
			err := service.db.Put(req.RowKey, m.SetCell.Value, nil)
			if err != nil {
				return nil, err
			}
		default:
			panic("implement me")
		}
	}
	return &bigtable.MutateRowResponse{}, nil
}

func (MockBigtableService) MutateRows(*bigtable.MutateRowsRequest, bigtable.Bigtable_MutateRowsServer) error {
	panic("implement me")
}

func (MockBigtableService) CheckAndMutateRow(context.Context, *bigtable.CheckAndMutateRowRequest) (*bigtable.CheckAndMutateRowResponse, error) {
	panic("implement me")
}

func (MockBigtableService) ReadModifyWriteRow(context.Context, *bigtable.ReadModifyWriteRowRequest) (*bigtable.ReadModifyWriteRowResponse, error) {
	panic("implement me")
}
