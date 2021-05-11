package metadata

import (
	"errors"
	"fmt"
	"sync/atomic"
)

func NewSegment(bucket_id, id uint64) *Segment {
	seg := &Segment{
		BucketID: bucket_id,
		ID:       ID{ID: id},
		Blocks:   make(map[uint64]*Block),
	}
	return seg
}

func (seg *Segment) GetBucketID() uint64 {
	return seg.BucketID
}

func (seg *Segment) BlockIDs() map[uint64]ID {
	ids := make(map[uint64]ID)
	for _, blk := range seg.Blocks {
		ids[blk.ID.ID] = blk.ID
	}
	return ids
}

func (seg *Segment) NextBlock() (blk *Block, err error) {
	blk_id := atomic.LoadUint64(&(seg.NextBlockID))
	// ok := atomic.CompareAndSwapUint64(&(seg.NextBlockID), blk_id, blk_id+1)
	// for ok != true {
	// 	blk_id = atomic.LoadUint64(&(seg.NextBlockID))
	// 	ok = atomic.CompareAndSwapUint64(&(seg.NextBlockID), blk_id, blk_id+1)
	// }

	blk = NewBlock(seg.BucketID, seg.ID.ID, blk_id)
	return blk, err
}

func (seg *Segment) String() string {
	s := fmt.Sprintf("Seg(%d-%s,NBlk=%d)", seg.BucketID, seg.ID.String(), seg.NextBlockID)
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

func (seg *Segment) Copy() *Segment {
	new_seg := NewSegment(seg.BucketID, seg.ID.ID)
	new_seg.ID = seg.ID
	new_seg.TimeStamp = seg.TimeStamp
	new_seg.State = seg.State
	new_seg.NextBlockID = seg.NextBlockID
	for k, v := range seg.Blocks {
		new_seg.Blocks[k] = v.Copy()
	}

	return new_seg
}
