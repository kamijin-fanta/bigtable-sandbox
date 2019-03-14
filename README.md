### technical memo

- https://github.com/googleapis/googleapis/blob/master/google/bigtable/v2/bigtable.proto
- https://github.com/googleapis/google-cloud-go/blob/master/bigtable/bttest/inmem.go

### milestone

- level 1
  - [x] MutateRow
  - [x] ReadRows(basic support)
  - [x] ReadRows(range support)
- level 2
  - [x] Column Family
  - [] ReadRows(filtered request support)
  - [] MutateRows
  - [] SampleRowKeys
  - [] CheckAndMutateRow
  - [] ReadModifyWriteRow
- level 3
  - [] ReadRows(advanced filter support)
  - [] Timestamp
- level 4
  - [] Version, GC
  - [] Statistics
  - [] Distribute
  - [] BigCell(ValueSize)
