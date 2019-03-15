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

func KeyEncoder(table string, rowKey []byte, family string, column []byte) []byte {
	// z:TABLE:ROW_KEY:FAMILY_NAME:COLUMN_NAME
	key := []byte("z:")
	key = append(key, []byte(table)...)
	if len(rowKey) != 0 { // todo support to empty RowKey
		key = append(key, []byte(":")...)
		key = append(key, rowKey...)
		if family != "" {
			key = append(key, []byte(":")...)
			key = append(key, []byte(family)...)
			if len(column) != 0 {
				key = append(key, []byte(":")...)
				key = append(key, []byte(column)...)
			}
		}
	}
	return key
}

func KeyDecoder(encodedKey []byte) (table string, rowKey []byte, family string, column []byte) {
	tablePos := bytes.IndexByte(encodedKey, byte(':'))
	if tablePos != -1 {
		rowKeyPos := bytes.IndexByte(encodedKey[tablePos+1:], byte(':'))
		if rowKeyPos != -1 {
			rowKeyPos += tablePos + 1
			familyPos := bytes.IndexByte(encodedKey[rowKeyPos+1:], byte(':'))
			if familyPos != -1 {
				familyPos += rowKeyPos + 1
				columnPos := bytes.IndexByte(encodedKey[familyPos+1:], byte(':'))
				if columnPos != -1 {
					columnPos += familyPos + 1
					table = string(encodedKey[tablePos+1 : rowKeyPos])
					rowKey = encodedKey[rowKeyPos+1 : familyPos]
					family = string(encodedKey[familyPos+1 : columnPos])
					column = encodedKey[columnPos+1:]
				}
			}
		}
	}

	return
}

type MockBigtableService struct {
	db *leveldb.DB
}

func Stream(iterCondition func() bool, iterVal, iterKey func() []byte) {

}

func makeChunk(filter *bigtable.RowFilter, rowStat []*CellStatus) (chunks []*bigtable.ReadRowsResponse_CellChunk, err error) {
	filtered, err := FilterRow(filter, rowStat)
	if err != nil {
		return
	}
	for i := range filtered {
		chunks = append(chunks, &bigtable.ReadRowsResponse_CellChunk{
			RowKey:          filtered[i].Key,
			Value:           filtered[i].Value,
			FamilyName:      &wrappers.StringValue{Value: filtered[i].Family},
			Qualifier:       &wrappers.BytesValue{Value: []byte(filtered[i].Column)},
			TimestampMicros: 0,
			Labels:          filtered[i].Label,
		})
	}
	if len(chunks) > 0 {
		chunks[len(chunks)-1].RowStatus = &bigtable.ReadRowsResponse_CellChunk_CommitRow{true}
	}
	return
}

func (service *MockBigtableService) ReadRows(req *bigtable.ReadRowsRequest, server bigtable.Bigtable_ReadRowsServer) error {
	var chunks []*bigtable.ReadRowsResponse_CellChunk

	// single Key requests
	for _, rowKey := range req.Rows.RowKeys {
		startKey := KeyEncoder(req.TableName, rowKey, "", []byte{})
		endKey := append(startKey, 255)
		iter := service.db.NewIterator(&util.Range{Start: startKey, Limit: endKey}, nil)
		var rowStat []*CellStatus
		for iter.Next() {
			key := make([]byte, len(iter.Key()))
			copy(key, iter.Key())
			value := make([]byte, len(iter.Value()))
			copy(value, iter.Value())
			_, _, family, column := KeyDecoder(key)
			rowStat = append(rowStat, &CellStatus{
				EncodedKey: key,
				Key:        rowKey,
				Family:     family,
				Column:     column,
				Value:      value,
			})
		}
		// todo req.RowsLimit
		filtered, err := makeChunk(req.Filter, rowStat)
		if err != nil {
			return err
		}
		chunks = append(chunks, filtered...)
	}

	// range request
	for _, reqRange := range req.Rows.RowRanges {
		var startKey []byte
		switch k := reqRange.StartKey.(type) {
		case *bigtable.RowRange_StartKeyClosed:
			startKey = KeyEncoder(req.TableName, k.StartKeyClosed, "", []byte{})
			startKey = append(startKey, 255)
		case *bigtable.RowRange_StartKeyOpen:
			startKey = KeyEncoder(req.TableName, k.StartKeyOpen, "", []byte{})
		}
		var endKey []byte
		switch k := reqRange.EndKey.(type) {
		case *bigtable.RowRange_EndKeyClosed:
			endKey = KeyEncoder(req.TableName, k.EndKeyClosed, "", []byte{})
			endKey = append(endKey, 0)
		case *bigtable.RowRange_EndKeyOpen:
			endKey = KeyEncoder(req.TableName, k.EndKeyOpen, "", []byte{})
			endKey = append(endKey, 255)
		case nil:
			endKey = []byte{255, 255, 255}
		}

		iter := service.db.NewIterator(&util.Range{Start: startKey, Limit: endKey}, nil)
		var rowStat []*CellStatus
		var lastKey []byte
		for iter.Next() {
			key := make([]byte, len(iter.Key()))
			copy(key, iter.Key())
			value := make([]byte, len(iter.Value()))
			copy(value, iter.Value())
			_, rowKey, family, column := KeyDecoder(key)

			if lastKey != nil && !bytes.Equal(lastKey, key) {
				filtered, err := makeChunk(req.Filter, rowStat)
				if err != nil {
					return err
				}
				chunks = append(chunks, filtered...)
				rowStat = []*CellStatus{}
			}

			rowStat = append(rowStat, &CellStatus{
				EncodedKey: key,
				Key:        rowKey,
				Family:     family,
				Column:     column,
				Value:      value,
			})
			lastKey = key
		}
		// todo req.RowsLimit
		filtered, err := makeChunk(req.Filter, rowStat)
		if err != nil {
			return err
		}
		chunks = append(chunks, filtered...)
	}

	res := bigtable.ReadRowsResponse{
		LastScannedRowKey: []byte{},
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
			encoded := KeyEncoder(req.TableName, req.RowKey, m.SetCell.FamilyName, m.SetCell.ColumnQualifier)
			err := service.db.Put(encoded, m.SetCell.Value, nil)
			if err != nil {
				return nil, err
			}
		case *bigtable.Mutation_DeleteFromRow_:
			start := KeyEncoder(req.TableName, req.RowKey, "", []byte{})
			end := append(start, 255)
			err := RangeDelete(service.db, start, end)
			if err != nil {
				return nil, err
			}
		case *bigtable.Mutation_DeleteFromFamily_:
			start := KeyEncoder(req.TableName, req.RowKey, m.DeleteFromFamily.FamilyName, []byte{})
			end := append(start, 255)
			err := RangeDelete(service.db, start, end)
			if err != nil {
				return nil, err
			}
		case *bigtable.Mutation_DeleteFromColumn_:
			start := KeyEncoder(req.TableName, req.RowKey, m.DeleteFromColumn.FamilyName, m.DeleteFromColumn.ColumnQualifier)
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
