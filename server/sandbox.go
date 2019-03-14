package main

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
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

func KeyEncoder(rowKey []byte, family string, column []byte) []byte {
	// z:ROW_KEY:FAMILY_NAME:COLUMN_NAME
	key := []byte("z:")
	key = append(key, rowKey...)
	if family != "" {
		key = append(key, []byte(":")...)
		key = append(key, []byte(family)...)
		if len(column) != 0 {
			key = append(key, []byte(":")...)
			key = append(key, []byte(column)...)
		}
	}
	return key
}

func KeyDecoder(encodedKey []byte) (rowKey []byte, family string, column []byte) {
	rowKeyPos := bytes.IndexByte(encodedKey, byte(':'))
	if rowKeyPos != -1 {
		familyPos := bytes.IndexByte(encodedKey[rowKeyPos+1:], byte(':'))
		if familyPos != -1 {
			familyPos += rowKeyPos + 1
			columnPos := bytes.IndexByte(encodedKey[familyPos+1:], byte(':'))
			if columnPos != -1 {
				columnPos += familyPos + 1
				rowKey = encodedKey[rowKeyPos+1 : familyPos]
				family = string(encodedKey[familyPos+1 : columnPos])
				column = encodedKey[columnPos+1:]
			}
		}
	}

	return
}

type MockBigtableService struct {
	db *leveldb.DB
}

func (service *MockBigtableService) ReadRows(req *bigtable.ReadRowsRequest, server bigtable.Bigtable_ReadRowsServer) error {
	var chunks []*bigtable.ReadRowsResponse_CellChunk

	// single key requests
	for _, rowKey := range req.Rows.RowKeys {
		startKey := KeyEncoder(rowKey, "", []byte{})
		endKey := append(startKey, 255)
		iter := service.db.NewIterator(&util.Range{Start: startKey, Limit: endKey}, nil)
		for iter.Next() {
			_, family, column := KeyDecoder(iter.Key())
			chunks = append(chunks, &bigtable.ReadRowsResponse_CellChunk{
				RowKey:          rowKey,
				Value:           iter.Value(),
				FamilyName:      &wrappers.StringValue{Value: family},
				Qualifier:       &wrappers.BytesValue{Value: []byte(column)},
				TimestampMicros: 0,
				Labels:          []string{"label"},
			})
		}
		// todo req.RowsLimit
	}
	for range req.Rows.RowRanges {
		// todo range request
	}
	// todo req.Filter

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
			encoded := KeyEncoder(req.RowKey, m.SetCell.FamilyName, m.SetCell.ColumnQualifier)
			err := service.db.Put(encoded, m.SetCell.Value, nil)
			if err != nil {
				return nil, err
			}
		case *bigtable.Mutation_DeleteFromRow_:
			start := KeyEncoder(req.RowKey, "", []byte{})
			end := append(start, 255)
			err := RangeDelete(service.db, start, end)
			if err != nil {
				return nil, err
			}
		case *bigtable.Mutation_DeleteFromFamily_:
			start := KeyEncoder(req.RowKey, m.DeleteFromFamily.FamilyName, []byte{})
			end := append(start, 255)
			err := RangeDelete(service.db, start, end)
			if err != nil {
				return nil, err
			}
		case *bigtable.Mutation_DeleteFromColumn_:
			start := KeyEncoder(req.RowKey, m.DeleteFromColumn.FamilyName, m.DeleteFromColumn.ColumnQualifier)
			end := append(start, 255)
			err := RangeDelete(service.db, start, end)
			if err != nil {
				return nil, err
			}
		default:
			panic("implement me")
		}
	}
	return &bigtable.MutateRowResponse{}, nil
}
func RangeDelete(db *leveldb.DB, start, end []byte) error {
	iter := db.NewIterator(&util.Range{Start: start, Limit: end}, nil)
	for iter.Next() {
		err := db.Delete(iter.Key(), nil)
		if err != nil {
			return err
		}
	}
	return nil
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
