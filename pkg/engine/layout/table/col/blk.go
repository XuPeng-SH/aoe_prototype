package col

import (
	"aoe/pkg/engine/layout"
	"io"
)

type IColumnBlock interface {
	io.Closer
	GetNext() IColumnBlock
	SetNext(next IColumnBlock)
	GetID() layout.BlockId
	GetRowCount() uint64
	GetSegment() IColumnSegment
	InitScanCursor(cusor *ScanCursor) error
	Append(part IColumnPart)
	GetPartRoot() IColumnPart
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

func (blk *ColumnBlock) SetNext(next IColumnBlock) {
	// if blk.Next != nil {
	// 	panic("logic error")
	// }
	blk.Next = next
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
