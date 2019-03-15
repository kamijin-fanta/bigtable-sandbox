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
  - [x] ReadRows(filtered request support)
  - [ ] MutateRows
  - [ ] CheckAndMutateRow
  - [ ] ReadModifyWriteRow
- level 3
  - [ ] ReadRows(advanced filter support)
  - [ ] Timestamp
  - [ ] Family
- level 4
  - [ ] Version, GC
  - [ ] Statistics
  - [ ] Distribute
  - [ ] BigCell(ValueSize)
  - [ ] SampleRowKeys
  - [ ] Limitation/Validation
    - SoftLimit
      - KeySize: 4KB
      - Families per table: 100
      - Column Qualifier: 16KB
      - Cell size: 10MB
      - Row size: 100MB
    - HardLimit
      - Cell size: 100MB
      - Row: 256MB 
