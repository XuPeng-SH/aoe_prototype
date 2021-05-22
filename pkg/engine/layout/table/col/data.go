package col

import (
	"fmt"
)

type IColumnData interface {
}

type ColumnData struct {
	Type     interface{}
	Idx      uint64
	RowCount uint64
	SegTree  ISegmentTree
}

func NewColumnData(col_type interface{}, col_idx uint64) IColumnData {
	data := &ColumnData{
		Type: col_type,
		Idx:  col_idx,
	}
	return data
}

func (cdata *ColumnData) String() string {
	return fmt.Sprintf("CData(%d,%d,%d)", cdata.Type, cdata.Idx, cdata.RowCount)
}
