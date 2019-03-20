package main

import (
	"bytes"
	"fmt"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math/rand"
	"regexp"
)

type CellStatus struct {
	EncodedKey []byte
	Key        []byte
	Family     string
	Column     []byte
	Label      []string
	Value      []byte
}

func FilterRow(filter *bigtable.RowFilter, row []*CellStatus) ([]*CellStatus, error) {
	// must be row[0].Key == row[n].Key

	if filter == nil || len(row) == 0 {
		return row, nil
	}
	switch f := filter.Filter.(type) {
	case *bigtable.RowFilter_BlockAllFilter:
		if f.BlockAllFilter {
			return nil, nil
		} else {
			return row, nil
		}
	case *bigtable.RowFilter_PassAllFilter:
		if f.PassAllFilter {
			return row, nil
		} else {
			return nil, nil
		}
	case *bigtable.RowFilter_Chain_:
		output := row
		for _, subFilter := range f.Chain.Filters {
			mod, err := FilterRow(subFilter, output)
			if err != nil {
				return nil, err
			}
			output = mod
		}
		// todo sort
		return output, nil
	case *bigtable.RowFilter_Interleave_:
		var output []*CellStatus
		for _, subFilter := range f.Interleave.Filters {
			mod, err := FilterRow(subFilter, row)
			if err != nil {
				return nil, err
			}
			output = append(output, mod...)
		}
		// todo sort
		return output, nil
	case *bigtable.RowFilter_CellsPerColumnLimitFilter:
		limit := int(f.CellsPerColumnLimitFilter)

		var output []*CellStatus
		var lastFamily string
		var lastColumn []byte
		var cellIndex int
		for i := range row {
			if lastFamily == row[i].Family && bytes.Equal(lastColumn, row[i].Column) {
				cellIndex++
			} else {
				lastFamily = row[i].Family
				lastColumn = row[i].Column
				cellIndex = 0
			}
			if cellIndex < limit {
				output = append(output, row[i])
			}
		}
		return output, nil
	case *bigtable.RowFilter_Condition_:
		result, err := FilterRow(f.Condition.PredicateFilter, row)
		if err != nil {
			return nil, err
		}
		if len(result) != 0 {
			if f.Condition.TrueFilter == nil {
				return nil, nil
			} else {
				return FilterRow(f.Condition.TrueFilter, row)
			}
		} else {
			if f.Condition.FalseFilter == nil {
				return nil, nil
			} else {
				return FilterRow(f.Condition.FalseFilter, row)
			}
		}
	case *bigtable.RowFilter_RowKeyRegexFilter:
		pattern, err := newRegexp(toUtf8(f.RowKeyRegexFilter))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid RowKeyRegexFilter: %v", err)
		}
		if pattern.MatchString(string(row[0].Key)) {
			return row, nil
		} else {
			return nil, nil
		}
	case *bigtable.RowFilter_CellsPerRowLimitFilter:
		limit := int(f.CellsPerRowLimitFilter)
		if limit > len(row) {
			limit = len(row)
		}
		return row[0:limit], nil
	case *bigtable.RowFilter_CellsPerRowOffsetFilter:
		offset := int(f.CellsPerRowOffsetFilter)
		if offset > len(row) {
			offset = len(row)
		}
		return row[offset:], nil
	case *bigtable.RowFilter_RowSampleFilter:
		if f.RowSampleFilter <= 0.0 || f.RowSampleFilter >= 1.0 {
			return nil, status.Error(codes.InvalidArgument, "row_sample_filter argument must be between 0.0 and 1.0")
		}
		if rand.Float64() < f.RowSampleFilter {
			return row, nil
		} else {
			return nil, nil
		}
		//case *bigtable.RowFilter_:
		//case *bigtable.RowFilter_:
		//case *bigtable.RowFilter_:
	}

	var output []*CellStatus
	for i := range row {
		cond, err := CellCondition(filter, row[i])
		if err != nil {
			return nil, err
		}
		if cond {
			modified, err := CellTransformer(filter, row[i])
			if err != nil {
				return nil, err
			}
			output = append(output, modified)
		}
	}

	return output, nil
}

func CellCondition(filter *bigtable.RowFilter, cell *CellStatus) (bool, error) {
	if filter == nil {
		return true, nil
	}

	switch f := filter.Filter.(type) {
	default:
		fmt.Printf("not implemented for %T handler", f)
		return true, nil
	case *bigtable.RowFilter_FamilyNameRegexFilter:
		pattern, err := newRegexp(f.FamilyNameRegexFilter)
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "Invalid RowKeyRegexFilter: %v", err)
		}
		return pattern.MatchString(cell.Family), nil
	case *bigtable.RowFilter_ColumnQualifierRegexFilter:
		pattern, err := newRegexp(toUtf8(f.ColumnQualifierRegexFilter))
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "Invalid RowKeyRegexFilter: %v", err)
		}
		return pattern.MatchString(toUtf8(cell.Column)), nil
	case *bigtable.RowFilter_ValueRegexFilter:
		pattern, err := newRegexp(toUtf8(f.ValueRegexFilter))
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "Invalid RowKeyRegexFilter: %v", err)
		}
		return pattern.MatchString(toUtf8(cell.Value)), nil
	case *bigtable.RowFilter_ColumnRangeFilter:
		if f.ColumnRangeFilter.FamilyName != cell.Family {
			return false, nil
		}
		start := true
		switch sub := f.ColumnRangeFilter.StartQualifier.(type) {
		case *bigtable.ColumnRange_StartQualifierOpen:
			// column > startQualifier
			start = bytes.Compare(cell.Column, sub.StartQualifierOpen) == 1
		case *bigtable.ColumnRange_StartQualifierClosed:
			// column >= startQualifier
			start = bytes.Compare(cell.Column, sub.StartQualifierClosed) != -1
		}

		end := true
		switch sub := f.ColumnRangeFilter.EndQualifier.(type) {
		case *bigtable.ColumnRange_EndQualifierOpen:
			// column < endQualifier
			end = bytes.Compare(cell.Column, sub.EndQualifierOpen) == -1
		case *bigtable.ColumnRange_EndQualifierClosed:
			// column <= endQualifier
			end = bytes.Compare(cell.Column, sub.EndQualifierClosed) != 1
		}

		return start && end, nil
	case *bigtable.RowFilter_TimestampRangeFilter:
		return true, nil
	case *bigtable.RowFilter_ValueRangeFilter:
		start := true
		switch sub := f.ValueRangeFilter.StartValue.(type) {
		case *bigtable.ValueRange_StartValueOpen:
			// value >= startQualifier
			start = bytes.Compare(cell.Value, sub.StartValueOpen) != -1
		case *bigtable.ValueRange_StartValueClosed:
			// value > startQualifier
			start = bytes.Compare(cell.Value, sub.StartValueClosed) == 1
		}

		end := true
		switch sub := f.ValueRangeFilter.EndValue.(type) {
		case *bigtable.ValueRange_EndValueOpen:
			// value <= endQualifier
			end = bytes.Compare(cell.Value, sub.EndValueOpen) != 1
		case *bigtable.ValueRange_EndValueClosed:
			// value < endQualifier
			end = bytes.Compare(cell.Value, sub.EndValueClosed) == -1
		}

		return start && end, nil
	}
}

func CellTransformer(filter *bigtable.RowFilter, cell *CellStatus) (*CellStatus, error) {
	if filter == nil {
		return cell, nil
	}

	switch f := filter.Filter.(type) {
	default:
		return cell, nil
	case *bigtable.RowFilter_StripValueTransformer:
		clonedCell := *cell
		clonedCell.Value = nil
		return &clonedCell, nil
	case *bigtable.RowFilter_ApplyLabelTransformer:
		clonedCell := *cell
		clonedCell.Label = []string{f.ApplyLabelTransformer}
		return &clonedCell, nil
	}
}

func toUtf8(stringByteArray []byte) string {
	var res []rune
	for _, b := range stringByteArray {
		res = append(res, rune(b))
	}
	return string(res)
}
func newRegexp(pattern string) (*regexp.Regexp, error) {
	result, err := regexp.Compile("^" + pattern + "$")
	return result, err
}
