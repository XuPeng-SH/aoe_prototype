package col

import (
	"aoe/pkg/engine/layout"
	"fmt"
)

type IColumnSegment interface {
	GetNext() IColumnSegment
	SetNext(next IColumnSegment)
	GetID() layout.BlockId
	GetBlockRoot() IColumnBlock
	GetPartRoot() IColumnPart
	GetRowCount() uint64
	String() string
	ToString(verbose bool) string
	Append(blk IColumnBlock)
}

type ColumnSegment struct {
	ID        layout.BlockId
	Next      IColumnSegment
	BlockRoot IColumnBlock
	BlockTail IColumnBlock
	RowCount  uint64
}

func NewSegment(id layout.BlockId) IColumnSegment {
	seg := &ColumnSegment{
		ID: id,
	}
	return seg
}

func (seg *ColumnSegment) GetRowCount() uint64 {
	return seg.RowCount
}

func (seg *ColumnSegment) SetNext(next IColumnSegment) {
	seg.Next = next
}

func (seg *ColumnSegment) GetNext() IColumnSegment {
	return seg.Next
}

func (seg *ColumnSegment) GetID() layout.BlockId {
	return seg.ID
}

func (seg *ColumnSegment) Append(blk IColumnBlock) {
	if !seg.ID.IsSameSegment(blk.GetID()) {
		panic("logic error")
	}
	if seg.BlockTail == nil {
		seg.BlockRoot = blk
		seg.BlockTail = blk
	} else {
		seg.BlockTail.SetNext(blk)
	}
	seg.RowCount += blk.GetRowCount()
}

func (seg *ColumnSegment) GetBlockRoot() IColumnBlock {
	return seg.BlockRoot
}

func (seg *ColumnSegment) GetPartRoot() IColumnPart {
	if seg.BlockRoot != nil {
		return seg.BlockRoot.GetPartRoot()
	}
	return nil
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
