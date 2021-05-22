package col

import (
	"aoe/pkg/engine/layout"
	"fmt"
)

type IColumnSegment interface {
	GetNext() IColumnSegment
	GetID() layout.BlockId
	GetBlockRoot() IColumnBlock
	GetRowCount() uint64
	String() string
	ToString(verbose bool) string
}

type ColumnSegment struct {
	ID        layout.BlockId
	Next      IColumnSegment
	BlockRoot IColumnBlock
	RowCount  uint64
}

func (seg *ColumnSegment) GetRowCount() uint64 {
	return seg.RowCount
}

func (seg *ColumnSegment) GetNext() IColumnSegment {
	return seg.Next
}

func (seg *ColumnSegment) GetID() layout.BlockId {
	return seg.ID
}

func (seg *ColumnSegment) GetBlockRoot() IColumnBlock {
	return seg.BlockRoot
}

func (seg *ColumnSegment) String() string {
	return seg.ToString(true)
}

func (seg *ColumnSegment) ToString(verbose bool) string {
	if verbose {
		return fmt.Sprintf("Seg(%v)(%d)[HasNext:%v]", seg.ID, seg.RowCount, seg.Next != nil)
	}
	return fmt.Sprintf("(%v, %v)", seg.ID, seg.RowCount)
}
