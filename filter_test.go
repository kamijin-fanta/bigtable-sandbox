package main

import (
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/bigtable/v2"
	"testing"
)

func TestFilter(t *testing.T) {
	rows := []*CellStatus{
		{
			Key:    []byte("k1"),
			Family: "cf1",
			Column: []byte("c1"),
			Value:  []byte("v1"),
		},
		{
			Key:    []byte("k1"),
			Family: "cf1",
			Column: []byte("c2"),
			Value:  []byte("v2"),
		},
	}

	t.Run("chain", func(t *testing.T) {
		ass := assert.New(t)
		filter := bigtable.RowFilter{
			Filter: &bigtable.RowFilter_Chain_{
				Chain: &bigtable.RowFilter_Chain{
					Filters: []*bigtable.RowFilter{
						{
							Filter: &bigtable.RowFilter_PassAllFilter{
								PassAllFilter: true,
							},
						},
					},
				},
			},
		}
		output, err := FilterRow(&filter, rows)
		ass.Nil(err)
		ass.Equal(2, len(output))
	})
	t.Run("interleave", func(t *testing.T) {
		ass := assert.New(t)
		filter := bigtable.RowFilter{
			Filter: &bigtable.RowFilter_Interleave_{
				Interleave: &bigtable.RowFilter_Interleave{
					Filters: []*bigtable.RowFilter{
						{
							Filter: &bigtable.RowFilter_PassAllFilter{
								PassAllFilter: true,
							},
						},
						{
							Filter: &bigtable.RowFilter_ValueRegexFilter{
								ValueRegexFilter: []byte(".2"),
							},
						},
					},
				},
			},
		}
		output, err := FilterRow(&filter, rows)
		ass.Nil(err)
		ass.Equal(3, len(output))
	})
	t.Run("value regex", func(t *testing.T) {
		ass := assert.New(t)
		filter := bigtable.RowFilter{
			Filter: &bigtable.RowFilter_ValueRegexFilter{
				ValueRegexFilter: []byte(".2"),
			},
		}
		output, err := FilterRow(&filter, rows)
		ass.Nil(err)
		ass.Equal(1, len(output))
		ass.Equal("v2", string(output[0].Value))
	})
}
