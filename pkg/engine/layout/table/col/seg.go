package col

import (
	"aoe/pkg/engine/layout"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	log "github.com/sirupsen/logrus"
)

type SegmentType uint8

const (
	UNSORTED_SEG SegmentType = iota
	SORTED_SEG
)

type IColumnSegment interface {
	io.Closer
	GetNext() IColumnSegment
	SetNext(next IColumnSegment)
	GetID() layout.ID
	GetBlockIDs() []layout.ID
	GetBlockRoot() IColumnBlock
	GetPartRoot() IColumnPart
	GetRowCount() uint64
	String() string
	ToString(verbose bool) string
	Append(blk IColumnBlock)
	GetColIdx() int
	GetSegmentType() SegmentType
	CloneWithUpgrade() IColumnSegment
	UpgradeBlock(id layout.ID) (IColumnBlock, error)
	GetBlock(id layout.ID) IColumnBlock
	InitScanCursor(cursor *ScanCursor) error
}

type ColumnSegment struct {
	sync.RWMutex
	ID       layout.ID
	Next     IColumnSegment
	Blocks   []IColumnBlock
	RowCount uint64
	IDMap    map[layout.ID]int
	Idx      int
	Type     SegmentType
}

func NewSegment(id layout.ID, colIdx int, segType SegmentType) IColumnSegment {
	seg := &ColumnSegment{
		ID:    id,
		IDMap: make(map[layout.ID]int, 0),
		Idx:   colIdx,
		Type:  segType,
	}
	runtime.SetFinalizer(seg, func(o IColumnSegment) {
		id := o.GetID()
		o.SetNext(nil)
		log.Infof("[GC]: ColumnSegment %s [%d]", id.SegmentString(), o.GetSegmentType())
		o.Close()
	})
	return seg
}

func (seg *ColumnSegment) GetColIdx() int {
	return seg.Idx
}

func (seg *ColumnSegment) GetSegmentType() SegmentType {
	seg.RLock()
	defer seg.RUnlock()
	return seg.Type
}

func (seg *ColumnSegment) GetBlock(id layout.ID) IColumnBlock {
	seg.RLock()
	defer seg.RUnlock()
	idx, ok := seg.IDMap[id]
	if !ok {
		return nil
	}
	return seg.Blocks[idx]
}

func (seg *ColumnSegment) UpgradeBlock(id layout.ID) (IColumnBlock, error) {
	if seg.Type != UNSORTED_SEG {
		panic("logic error")
	}
	if !seg.ID.IsSameSegment(id) {
		panic("logic error")
	}
	seg.Lock()
	defer seg.Unlock()
	idx, ok := seg.IDMap[id]
	if !ok {
		panic("logic error")
	}
	upgradeBlk := seg.Blocks[idx].CloneWithUpgrade(seg)
	if upgradeBlk == nil {
		return nil, errors.New(fmt.Sprintf("Cannot upgrade blk: %s", id.BlockString()))
	}
	if idx > 0 {
		seg.Blocks[idx-1].SetNext(upgradeBlk)
	}
	seg.Blocks[idx] = upgradeBlk
	return upgradeBlk, nil
}

func (seg *ColumnSegment) CloneWithUpgrade() IColumnSegment {
	if seg.Type != UNSORTED_SEG {
		panic("logic error")
	}
	cloned := &ColumnSegment{
		ID:       seg.ID,
		IDMap:    seg.IDMap,
		RowCount: seg.RowCount,
		Next:     seg.Next,
		// Blocks:   seg.Blocks,
	}
	var prev IColumnBlock
	for _, blk := range seg.Blocks {
		cur := blk.CloneWithUpgrade(cloned)
		cloned.Blocks = append(seg.Blocks, cur)
		if prev != nil {
			prev.SetNext(cur)
		}
		prev = cur
	}
	runtime.SetFinalizer(cloned, func(o IColumnSegment) {
		id := o.GetID()
		o.SetNext(nil)
		log.Infof("[GC]: ColumnSegment %s [%d]", id.SegmentString(), o.GetSegmentType())
	})
	cloned.Next = seg.Next
	return cloned
}

func (seg *ColumnSegment) GetRowCount() uint64 {
	seg.RLock()
	defer seg.RUnlock()
	return seg.RowCount
}

func (seg *ColumnSegment) SetNext(next IColumnSegment) {
	seg.Lock()
	defer seg.Unlock()
	seg.Next = next
}

func (seg *ColumnSegment) GetNext() IColumnSegment {
	seg.RLock()
	defer seg.RUnlock()
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
	seg.Lock()
	defer seg.Unlock()
	if len(seg.Blocks) > 0 {
		seg.Blocks[len(seg.Blocks)-1].SetNext(blk)
	}
	seg.Blocks = append(seg.Blocks, blk)
	seg.IDMap[blk.GetID()] = len(seg.Blocks) - 1
	seg.RowCount += blk.GetRowCount()
}

func (seg *ColumnSegment) GetBlockRoot() IColumnBlock {
	seg.RLock()
	defer seg.RUnlock()
	if len(seg.Blocks) == 0 {
		return nil
	}
	return seg.Blocks[0]
}

func (seg *ColumnSegment) GetPartRoot() IColumnPart {
	seg.RLock()
	defer seg.RUnlock()
	if len(seg.Blocks) == 0 {
		return nil
	}
	return seg.Blocks[0].GetPartRoot()
}

func (seg *ColumnSegment) InitScanCursor(cursor *ScanCursor) error {
	seg.RLock()
	if len(seg.Blocks) == 0 {
		return nil
	}
	blk := seg.Blocks[0]
	seg.RUnlock()
	return blk.InitScanCursor(cursor)
}

func (seg *ColumnSegment) GetBlockIDs() []layout.ID {
	seg.RLock()
	defer seg.RUnlock()
	var ids []layout.ID
	for _, blk := range seg.Blocks {
		ids = append(ids, blk.GetID())
	}
	return ids
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
