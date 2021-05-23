package col

import (
	"fmt"
)

type IColumnData interface {
	String() string
	InitScanCursor(cursor *ScanCursor) error
	Append(seg IColumnSegment) error
	// AppendBlock(blk IColumnBlock) error
	// AppendPart(part IColumnPart) error
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

// func (cdata *ColumnData) AppendBlock(blk IColumnBlock) error {
// 	tail_seg := cdata.SegTree.GetTail()
// 	id := blk.GetID()
// 	if tail_seg == nil || !id.IsSameSegment(tail_seg.GetID()) {
// 		err := cdata.Append(blk.GetSegment())
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (cdata *ColumnData) AppendPart(part IColumnPart) error {
// 	err := cdata.AppendBlock(part.GetBlock())
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (cdata *ColumnData) InitScanCursor(cursor *ScanCursor) error {
	err := cursor.Close()
	if err != nil {
		return err
	}
	root := cdata.SegTree.GetRoot()
	if root == nil {
		return nil
	}
	blk := root.GetBlockRoot()
	if blk == nil {
		return nil
	}
	cursor.Current = blk.GetPartRoot()
	return nil
}

func (cdata *ColumnData) String() string {
	return fmt.Sprintf("CData(%d,%d,%d)[SegCnt=%d]", cdata.Type, cdata.Idx, cdata.RowCount, cdata.SegmentCount())
}
