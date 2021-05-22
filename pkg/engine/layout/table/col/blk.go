package col

import (
	"aoe/pkg/engine/layout"
)

type IColumnBlock interface {
	GetNext() IColumnBlock
	GetID() layout.BlockId
	GetRowCount() uint64
	GetSegment() IColumnSegment
}

type ColumnBlock struct {
	ID       layout.BlockId
	Next     IColumnBlock
	Segment  IColumnSegment
	RowCount uint64
}

func (blk *ColumnBlock) GetSegment() IColumnSegment {
	return blk.Segment
}

func (blk *ColumnBlock) GetRowCount() uint64 {
	return blk.RowCount
}

func (blk *ColumnBlock) GetNext() IColumnBlock {
	n := blk.Next
	if n == nil {
		next_seg := blk.Segment.GetNext()
		if next_seg != nil {
			return next_seg.GetBlockRoot()
		}
	}
	return n
}

func (blk *ColumnBlock) GetID() layout.BlockId {
	return blk.ID
}
