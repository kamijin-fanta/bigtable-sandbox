package main

import (
	"bytes"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"golang.org/x/net/context"
	bigtableAdmin "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/genproto/googleapis/longrunning"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"strings"
)

type MockBigtableService struct {
	db Store
}

//////////////////// Keys ////////////////////

func TableKeyEncoder(tableId string) []byte {
	// t:TABLE
	key := []byte("t:")
	key = append(key, []byte(tableId)...)
	return key
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
	delimiter := byte(':')
	tablePos := bytes.IndexByte(encodedKey, delimiter)
	if tablePos != -1 {
		rowKeyPos := bytes.IndexByte(encodedKey[tablePos+1:], byte(':'))
		if rowKeyPos != -1 {
			rowKeyPos += tablePos + 1
			columnPos := bytes.LastIndexByte(encodedKey, delimiter)
			if columnPos != -1 {
				familyPos := bytes.LastIndexByte(encodedKey[:columnPos], delimiter)
				if familyPos != -1 && familyPos != rowKeyPos {
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

//////////////////// User API ////////////////////

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

func TableIdNormalize(tableName string) string {
	arr := strings.Split(tableName, "/")
	return arr[len(arr)-1]
}

func (service *MockBigtableService) ReadRows(req *bigtable.ReadRowsRequest, server bigtable.Bigtable_ReadRowsServer) error {
	var chunks []*bigtable.ReadRowsResponse_CellChunk

	tableName := TableIdNormalize(req.TableName)

	// single Key requests
	for _, rowKey := range req.Rows.RowKeys {
		startKey := KeyEncoder(tableName, rowKey, "", []byte{})
		endKey := append(startKey, 255)
		iter := service.db.RangeGet(startKey, endKey)
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
		case *bigtable.RowRange_StartKeyOpen:
			startKey = KeyEncoder(tableName, k.StartKeyOpen, "", []byte{})
			startKey = append(startKey, 255)
		case *bigtable.RowRange_StartKeyClosed:
			startKey = KeyEncoder(tableName, k.StartKeyClosed, "", []byte{})
		}
		var endKey []byte
		switch k := reqRange.EndKey.(type) {
		case *bigtable.RowRange_EndKeyOpen:
			endKey = KeyEncoder(tableName, k.EndKeyOpen, "", []byte{})
			endKey = append(endKey)
		case *bigtable.RowRange_EndKeyClosed:
			endKey = KeyEncoder(tableName, k.EndKeyClosed, "", []byte{})
			endKey = append(endKey, 255)
		case nil:
			endKey = []byte{255, 255, 255}
		}

		iter := service.db.RangeGet(startKey, endKey)
		var rowStat []*CellStatus
		var lastKey []byte
		for iter.Next() {
			key := make([]byte, len(iter.Key()))
			copy(key, iter.Key())
			value := make([]byte, len(iter.Value()))
			copy(value, iter.Value())
			_, rowKey, family, column := KeyDecoder(key)

			if lastKey != nil && !bytes.Equal(lastKey, rowKey) {
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
			lastKey = rowKey
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
	tableName := TableIdNormalize(req.TableName)
	for _, mutation := range req.Mutations {
		err := Mutation(service.db, tableName, req.RowKey, mutation)
		if err != nil {
			return nil, err
		}
	}
	return &bigtable.MutateRowResponse{}, nil
}
func Mutation(db Store, tableName string, rowKey []byte, mutation *bigtable.Mutation) error {
	switch m := mutation.Mutation.(type) {
	case *bigtable.Mutation_SetCell_:
		encoded := KeyEncoder(tableName, rowKey, m.SetCell.FamilyName, m.SetCell.ColumnQualifier)
		err := db.Put(encoded, m.SetCell.Value)
		if err != nil {
			return err
		}
	case *bigtable.Mutation_DeleteFromRow_:
		start := KeyEncoder(tableName, rowKey, "", []byte{})
		end := append(start, 255)
		err := db.RangeDelete(start, end)
		if err != nil {
			return err
		}
	case *bigtable.Mutation_DeleteFromFamily_:
		start := KeyEncoder(tableName, rowKey, m.DeleteFromFamily.FamilyName, []byte{})
		end := append(start, 255)
		err := db.RangeDelete(start, end)
		if err != nil {
			return err
		}
	case *bigtable.Mutation_DeleteFromColumn_:
		start := KeyEncoder(tableName, rowKey, m.DeleteFromColumn.FamilyName, m.DeleteFromColumn.ColumnQualifier)
		end := append(start, 255)
		err := db.RangeDelete(start, end)
		if err != nil {
			return err
		}
	default:
		panic("implement me")
	}
	return nil
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

func (service *MockBigtableService) MutateRows(req *bigtable.MutateRowsRequest, server bigtable.Bigtable_MutateRowsServer) error {
	code, msg := int32(codes.OK), ""
	tableName := TableIdNormalize(req.TableName)
	response := bigtable.MutateRowsResponse{
		Entries: make([]*bigtable.MutateRowsResponse_Entry, len(req.Entries)),
	}
	for i, entry := range req.Entries {
		for _, mutate := range entry.Mutations {
			if err := Mutation(service.db, tableName, entry.RowKey, mutate); err != nil {
				code = int32(codes.Internal)
				msg = err.Error()
			}
		}
		response.Entries[i] = &bigtable.MutateRowsResponse_Entry{
			Index: int64(i),
			Status: &spb.Status{
				Code:    code,
				Message: msg,
			},
		}
	}
	return server.Send(&response)
}

func (MockBigtableService) CheckAndMutateRow(context.Context, *bigtable.CheckAndMutateRowRequest) (*bigtable.CheckAndMutateRowResponse, error) {
	panic("implement me")
}

func (MockBigtableService) ReadModifyWriteRow(context.Context, *bigtable.ReadModifyWriteRowRequest) (*bigtable.ReadModifyWriteRowResponse, error) {
	panic("implement me")
}

//////////////////// Admin API ////////////////////

func (service *MockBigtableService) CreateTable(ctx context.Context, req *bigtableAdmin.CreateTableRequest) (*bigtableAdmin.Table, error) {
	id := req.Parent + "/tables/" + req.TableId
	req.Table.Name = id
	tableBytes, err := proto.Marshal(req.Table)
	if err != nil {
		return nil, err
	}
	key := TableKeyEncoder(req.TableId)
	err = service.db.Put(key, tableBytes)
	if err != nil {
		return nil, err
	}
	return req.Table, nil
}

func (service *MockBigtableService) CreateTableFromSnapshot(context.Context, *bigtableAdmin.CreateTableFromSnapshotRequest) (*longrunning.Operation, error) {
	panic("implement me")
}

func (service *MockBigtableService) ListTables(context.Context, *bigtableAdmin.ListTablesRequest) (*bigtableAdmin.ListTablesResponse, error) {
	panic("implement me")
}

func (service *MockBigtableService) GetTable(ctx context.Context, req *bigtableAdmin.GetTableRequest) (*bigtableAdmin.Table, error) {
	key := TableKeyEncoder(TableIdNormalize(req.Name))
	tableBytes, err := service.db.Get(key)
	if err != nil {
		return nil, err
	}
	var table bigtableAdmin.Table
	err = proto.Unmarshal(tableBytes, &table)
	if err != nil {
		return nil, err
	}
	return &table, nil
}

func (service *MockBigtableService) DeleteTable(context.Context, *bigtableAdmin.DeleteTableRequest) (*empty.Empty, error) {
	panic("implement me")
}

func (service *MockBigtableService) ModifyColumnFamilies(ctx context.Context, req *bigtableAdmin.ModifyColumnFamiliesRequest) (*bigtableAdmin.Table, error) {
	key := TableKeyEncoder(TableIdNormalize(req.Name))
	tableBytes, err := service.db.Get(key)
	if err != nil {
		return nil, err
	}
	var table bigtableAdmin.Table
	err = proto.Unmarshal(tableBytes, &table)
	if err != nil {
		return nil, err
	}

	if table.ColumnFamilies == nil {
		table.ColumnFamilies = map[string]*bigtableAdmin.ColumnFamily{}
	}
	for _, mod := range req.Modifications {
		switch m := mod.Mod.(type) {
		case *bigtableAdmin.ModifyColumnFamiliesRequest_Modification_Create:
			table.ColumnFamilies[mod.Id] = m.Create
		case *bigtableAdmin.ModifyColumnFamiliesRequest_Modification_Update:
			table.ColumnFamilies[mod.Id] = m.Update
		case *bigtableAdmin.ModifyColumnFamiliesRequest_Modification_Drop:
			delete(table.ColumnFamilies, mod.Id)
		}
	}

	tableBytes, err = proto.Marshal(&table)
	if err != nil {
		return nil, err
	}

	err = service.db.Put(key, tableBytes)
	if err != nil {
		return nil, err
	}

	return &table, nil
}

func (service *MockBigtableService) DropRowRange(context.Context, *bigtableAdmin.DropRowRangeRequest) (*empty.Empty, error) {
	panic("implement me")
}

func (service *MockBigtableService) GenerateConsistencyToken(context.Context, *bigtableAdmin.GenerateConsistencyTokenRequest) (*bigtableAdmin.GenerateConsistencyTokenResponse, error) {
	panic("implement me")
}

func (service *MockBigtableService) CheckConsistency(context.Context, *bigtableAdmin.CheckConsistencyRequest) (*bigtableAdmin.CheckConsistencyResponse, error) {
	panic("implement me")
}

func (service *MockBigtableService) SnapshotTable(context.Context, *bigtableAdmin.SnapshotTableRequest) (*longrunning.Operation, error) {
	panic("implement me")
}

func (service *MockBigtableService) GetSnapshot(context.Context, *bigtableAdmin.GetSnapshotRequest) (*bigtableAdmin.Snapshot, error) {
	panic("implement me")
}

func (service *MockBigtableService) ListSnapshots(context.Context, *bigtableAdmin.ListSnapshotsRequest) (*bigtableAdmin.ListSnapshotsResponse, error) {
	panic("implement me")
}

func (service *MockBigtableService) DeleteSnapshot(context.Context, *bigtableAdmin.DeleteSnapshotRequest) (*empty.Empty, error) {
	panic("implement me")
}
