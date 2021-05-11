package metadata

import (
	"errors"
	"fmt"
	"sync/atomic"
)

func NewSegment(id uint64) *Segment {
	seg := &Segment{
		ID:     ID{ID: id},
		Blocks: make(map[uint64]*Block),
	}
	return seg
}

func (seg *Segment) NextBlock() (blk *Block, err error) {
	blk_id := atomic.LoadUint64(&(seg.NextBlockID))
	// ok := atomic.CompareAndSwapUint64(&(seg.NextBlockID), blk_id, blk_id+1)
	// for ok != true {
	// 	blk_id = atomic.LoadUint64(&(seg.NextBlockID))
	// 	ok = atomic.CompareAndSwapUint64(&(seg.NextBlockID), blk_id, blk_id+1)
	// }

	blk = NewBlock(blk_id)
	return blk, err
}

func (seg *Segment) String() string {
	s := fmt.Sprintf("Seg(%s,NBlk=%d)", seg.ID.String(), seg.NextBlockID)
	s += "["
	for i, blk := range seg.Blocks {
		if i != 0 {
			s += ","
		}
		s += blk.String()
	}
	s += "]"
	return s
}

func (seg *Segment) GetNextBlockID() uint64 {
	return seg.NextBlockID
}

func (seg *Segment) AddBlock(blk *Block) error {
	if seg.NextBlockID != blk.ID.ID {
		return errors.New(fmt.Sprintf("AddBlock %d is mismatch with NextBlockID %d", blk.ID.ID, seg.NextBlockID))
	}
	seg.Blocks[blk.ID.ID] = blk
	seg.NextBlockID += 1
	return nil
}
