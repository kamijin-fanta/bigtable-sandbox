package services

import (
	"bytes"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"golang.org/x/net/context"
	bigtableAdmin "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/genproto/googleapis/longrunning"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"strings"
)

type MockBigtableService struct {
	Db Store
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
	key := make([]byte, len(encodedKey))
	copy(key, encodedKey)
	encodedKey = key
	delimiter := byte(':')
	tablePos := bytes.IndexByte(key, delimiter)
	if tablePos != -1 {
		rowKeyPos := bytes.IndexByte(key[tablePos+1:], byte(':'))
		if rowKeyPos != -1 {
			rowKeyPos += tablePos + 1
			columnPos := bytes.LastIndexByte(key, delimiter)
			if columnPos != -1 {
				familyPos := bytes.LastIndexByte(key[:columnPos], delimiter)
				if familyPos != -1 && familyPos != rowKeyPos {
					table = string(key[tablePos+1 : rowKeyPos])
					rowKey = key[rowKeyPos+1 : familyPos]
					family = string(key[familyPos+1 : columnPos])
					column = key[columnPos+1:]
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
		iter := service.Db.RangeGet(startKey, endKey)
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

		iter := service.Db.RangeGet(startKey, endKey)
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

	mu := NewMutator(500, service.Db)
	err := mu.Mutation(tableName, req.RowKey, req.Mutations)
	if err != nil {
		return nil, err
	}
	err = mu.Commit()
	if err != nil {
		return nil, err
	}
	return &bigtable.MutateRowResponse{}, nil
}

type mutationSet struct {
	tableName string
	rowKey    []byte
	mutation  *bigtable.Mutation
}
type mutator struct {
	batchLength int
	queue       []mutationSet
	store       Store

	batchPutKeys   [][]byte
	batchPutValues [][]byte
}

func NewMutator(batchLength int, store Store) *mutator {
	return &mutator{
		batchLength: batchLength,
		store:       store,
	}
}
func (m *mutator) Mutation(tableName string, rowKey []byte, mutations []*bigtable.Mutation) error {
	if m.queue == nil {
		m.queue = make([]mutationSet, 0, m.batchLength+100)
	}
	for _, mutation := range mutations {
		m.queue = append(m.queue, mutationSet{
			tableName: tableName,
			rowKey:    rowKey,
			mutation:  mutation,
		})
	}

	if len(m.queue) > m.batchLength {
		return m.Commit()
	}
	return nil
}

func (m *mutator) Commit() error {
	if m.batchPutKeys == nil {
		m.batchPutKeys = make([][]byte, 0, m.batchLength+100)
	}
	if m.batchPutValues == nil {
		m.batchPutValues = make([][]byte, 0, m.batchLength+100)
	}
	for _, item := range m.queue {
		switch mu := item.mutation.Mutation.(type) {
		case *bigtable.Mutation_SetCell_:
			encoded := KeyEncoder(item.tableName, item.rowKey, mu.SetCell.FamilyName, mu.SetCell.ColumnQualifier)
			m.batchPutKeys = append(m.batchPutKeys, encoded)
			m.batchPutValues = append(m.batchPutValues, mu.SetCell.Value)
		case *bigtable.Mutation_DeleteFromRow_:
			start := KeyEncoder(item.tableName, item.rowKey, "", []byte{})
			end := append(start, 255)
			err := m.store.RangeDelete(start, end)
			if err != nil {
				return err
			}
		case *bigtable.Mutation_DeleteFromFamily_:
			start := KeyEncoder(item.tableName, item.rowKey, mu.DeleteFromFamily.FamilyName, []byte{})
			end := append(start, 255)
			err := m.store.RangeDelete(start, end)
			if err != nil {
				return err
			}
		case *bigtable.Mutation_DeleteFromColumn_:
			start := KeyEncoder(item.tableName, item.rowKey, mu.DeleteFromColumn.FamilyName, mu.DeleteFromColumn.ColumnQualifier)
			end := append(start, 255)
			err := m.store.RangeDelete(start, end)
			if err != nil {
				return err
			}
		default:
			panic("implement me")
		}
	}
	if len(m.batchPutKeys) != 0 {
		err := m.store.BatchPut(m.batchPutKeys, m.batchPutValues)
		if err != nil {
			return err
		}
		m.batchPutKeys = m.batchPutKeys[:0]
		m.batchPutValues = m.batchPutValues[:0]
	}
	return nil
}

func (service *MockBigtableService) MutateRows(req *bigtable.MutateRowsRequest, server bigtable.Bigtable_MutateRowsServer) error {
	tableName := TableIdNormalize(req.TableName)
	response := bigtable.MutateRowsResponse{
		Entries: make([]*bigtable.MutateRowsResponse_Entry, len(req.Entries)),
	}

	mu := NewMutator(500, service.Db)
	for i, entry := range req.Entries {
		code, msg := int32(codes.OK), ""
		if err := mu.Mutation(tableName, entry.RowKey, entry.Mutations); err != nil {
			code = int32(codes.Internal)
			msg = err.Error()
		}
		// last
		if len(req.Entries)-1 == i {
			err := mu.Commit()
			if err != nil {
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
	err = service.Db.Put(key, tableBytes)
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
	tableBytes, err := service.Db.Get(key)
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
	tableBytes, err := service.Db.Get(key)
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

	err = service.Db.Put(key, tableBytes)
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
