package col

import (
	"aoe/pkg/engine/layout"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"runtime"
	"sync"
)

// type SegmentType uint8

// const (
// 	UNCLOSED_SEG SegmentType = iota
// 	CLOSED_SEG
// 	SORTED_SEG
// )

type IColumnSegment interface {
	io.Closer
	GetNext() IColumnSegment
	SetNext(next IColumnSegment)
	GetID() layout.ID
	GetBlockRoot() IColumnBlock
	GetPartRoot() IColumnPart
	GetRowCount() uint64
	String() string
	ToString(verbose bool) string
	Append(blk IColumnBlock)
	UpgradeBlock(layout.ID)
}

type ColumnSegment struct {
	sync.RWMutex
	ID       layout.ID
	Next     IColumnSegment
	Blocks   []IColumnBlock
	RowCount uint64
	IDMap    map[layout.ID]int
}

func NewSegment(id layout.ID) IColumnSegment {
	seg := &ColumnSegment{
		ID:    id,
		IDMap: make(map[layout.ID]int, 0),
	}
	runtime.SetFinalizer(seg, func(o IColumnSegment) {
		id := o.GetID()
		o.SetNext(nil)
		log.Infof("[GC]: ColumnSegment %s", id.SegmentString())
	})
	return seg
}

func (seg *ColumnSegment) UpgradeBlock(blkID layout.ID) {
	// idx, ok := seg.IDMap[blkID]
	// if !ok {
	// 	panic("logic error")
	// }
	// blk := seg.Blocks[idx]
	// if blk.GetBlockType() != TRANSIENT_BLK {
	// 	panic("logic error")
	// }
	// if idx == 0 {

	// } else {
	// 	prev := seg.Blocks[idx-1]

	// }

	// if idx == len(seg.Blocks)-1 {

	// } else {

	// }
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

func (seg *ColumnSegment) Close() error {
	for _, blk := range seg.Blocks {
		err := blk.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (seg *ColumnSegment) GetID() layout.ID {
	return seg.ID
}

func (seg *ColumnSegment) Append(blk IColumnBlock) {
	if !seg.ID.IsSameSegment(blk.GetID()) {
		panic("logic error")
	}
	if len(seg.Blocks) > 0 {
		seg.Blocks[len(seg.Blocks)-1].SetNext(blk)
	}
	seg.Blocks = append(seg.Blocks, blk)
	seg.IDMap[blk.GetID()] = len(seg.Blocks) - 1
	seg.RowCount += blk.GetRowCount()
}

func (seg *ColumnSegment) GetBlockRoot() IColumnBlock {
	if len(seg.Blocks) == 0 {
		return nil
	}
	return seg.Blocks[0]
}

func (seg *ColumnSegment) GetPartRoot() IColumnPart {
	if len(seg.Blocks) == 0 {
		return nil
	}
	return seg.Blocks[0].GetPartRoot()
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
