package col

import (
	"fmt"
)

type IColumnData interface {
	String() string
	InitScanCursor(cursor *ScanCursor) error
	Append(seg IColumnSegment) error
	SegmentCount() uint64
	GetSegmentRoot() IColumnSegment
}

type ColumnData struct {
	Type     interface{}
	Idx      uint64
	RowCount uint64
	SegTree  ISegmentTree
}

func NewColumnData(col_type interface{}, col_idx uint64) IColumnData {
	data := &ColumnData{
		Type:    col_type,
		Idx:     col_idx,
		SegTree: NewSegmentTree(),
	}
	return data
}

func (cdata *ColumnData) GetSegmentRoot() IColumnSegment {
	return cdata.SegTree.GetRoot()
}

func (cdata *ColumnData) SegmentCount() uint64 {
	return cdata.SegTree.Depth()
}

func (cdata *ColumnData) Append(seg IColumnSegment) error {
	return cdata.SegTree.Append(seg)
}

func (cdata *ColumnData) InitScanCursor(cursor *ScanCursor) error {
	err := cursor.Close()
	if err != nil {
		return err
	}
	root := cdata.SegTree.GetRoot()
	if root == nil {
		return nil
	}
	cursor.Current = root.GetBlockRoot()
	return nil
}

func (cdata *ColumnData) String() string {
	return fmt.Sprintf("CData(%d,%d,%d)[SegCnt=%d]", cdata.Type, cdata.Idx, cdata.RowCount, cdata.SegmentCount())
}
