package col

import (
	"aoe/pkg/engine/layout"
	"io"
	"sync"
)

type BlockType uint8

const (
	TRANSIENT_BLK BlockType = iota
	PERSISTENT_BLK
	PERSISTENT_SORTED_BLK
	MOCK_BLK
)

type IColumnBlock interface {
	io.Closer
	GetNext() IColumnBlock
	SetNext(next IColumnBlock)
	GetID() layout.ID
	GetRowCount() uint64
	GetSegment() IColumnSegment
	InitScanCursor(cusor *ScanCursor) error
	Append(part IColumnPart)
	GetPartRoot() IColumnPart
	GetBlockType() BlockType
	GetColIdx() int
	CloneWithUpgrade(IColumnSegment) IColumnBlock
}

type ColumnBlock struct {
	sync.RWMutex
	ID       layout.ID
	Next     IColumnBlock
	Segment  IColumnSegment
	RowCount uint64
	Type     BlockType
}

func (blk *ColumnBlock) GetColIdx() int {
	return blk.Segment.GetColIdx()
}

func (blk *ColumnBlock) GetBlockType() BlockType {
	blk.RLock()
	defer blk.RUnlock()
	return blk.Type
}

func (blk *ColumnBlock) GetSegment() IColumnSegment {
	blk.RLock()
	defer blk.RUnlock()
	return blk.Segment
}

func (blk *ColumnBlock) GetRowCount() uint64 {
	return blk.RowCount
}

func (blk *ColumnBlock) SetNext(next IColumnBlock) {
	blk.Lock()
	defer blk.Unlock()
	blk.Next = next
}

func (blk *ColumnBlock) GetNext() IColumnBlock {
	n := blk.Next
	blk.RLock()
	seg := blk.Segment
	blk.RUnlock()
	if n == nil {
		next_seg := seg.GetNext()
		if next_seg != nil {
			return next_seg.GetBlockRoot()
		}
	}
	return n
}

func (blk *ColumnBlock) GetID() layout.ID {
	return blk.ID
}
