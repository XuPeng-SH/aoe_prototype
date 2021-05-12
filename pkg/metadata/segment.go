package metadata

import (
	"errors"
	"fmt"
	"time"
)

const (
	SEGMENT_BLOCK_COUNT = 4
)

func NewSegment(bucket_id, id uint64) *Segment {
	now := time.Now().Unix()
	seg := &Segment{
		BucketID:      bucket_id,
		ID:            ID{ID: id},
		Blocks:        make(map[uint64]*Block),
		TimeStamp:     TimeStamp{CreatedOn: now, UpdatedOn: now},
		State:         State{Type: PENDING},
		MaxBlockCount: SEGMENT_BLOCK_COUNT,
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
	if len(seg.Blocks) >= int(seg.MaxBlockCount) {
		return nil, errors.New("No space for new block")
	}
	blk = NewBlock(seg.BucketID, seg.ID.ID, seg.NextBlockID)
	return blk, err
}

func (seg *Segment) GetActiveBlock() (*Block, error) {
	if !seg.IsActive() {
		return nil, errors.New("segment is closed")
	}
	min_blk_id := seg.NextBlockID
	for blk_id, itblk := range seg.Blocks {
		if blk_id < min_blk_id && itblk.IsActive() {
			min_blk_id = blk_id
		}
	}
	if min_blk_id == seg.NextBlockID {
		// Need create new block for this segment
		return nil, nil
	}
	return seg.Blocks[min_blk_id], nil
}

func (seg *Segment) IsActive() bool {
	if seg.DataState == EMPTY || seg.DataState == PARTIAL {
		return true
	}
	return false
}

func (seg *Segment) String() string {
	s := fmt.Sprintf("Seg[%s-%s](%d-%s,NBlk=%d)", seg.State.String(), ToString(seg.DataState), seg.BucketID, seg.ID.String(), seg.NextBlockID)
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

func (seg *Segment) GetBlock(id uint64) *Block {
	blk, ok := seg.Blocks[id]
	if !ok {
		return nil
	}
	return blk
}

func (seg *Segment) UpdateBlock(blk *Block) (*Block, error) {
	target_blk := seg.GetBlock(blk.ID.ID)
	if target_blk == nil {
		return nil, errors.New("Update block not found")
	}
	if target_blk.DataState >= blk.DataState {
		return nil, errors.New("Cannot update with higher DataState")
	}
	seg.Blocks[blk.ID.ID] = blk.Copy()
	if !blk.IsActive() {
		full_blocks := 0
		for _, itblk := range seg.Blocks {
			if !itblk.IsActive() {
				full_blocks++
			}
		}
		if full_blocks < int(seg.MaxBlockCount) {
			seg.DataState = PARTIAL
		} else {
			seg.DataState = FULL
		}
	}
	return seg.Blocks[blk.ID.ID], nil
}

func (seg *Segment) RegisterBlock(blk *Block) error {
	if seg.NextBlockID != blk.ID.ID {
		return errors.New(fmt.Sprintf("RegisterBlock %d is mismatch with NextBlockID %d", blk.ID.ID, seg.NextBlockID))
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
	new_seg.DataState = seg.DataState
	new_seg.NextBlockID = seg.NextBlockID
	for k, v := range seg.Blocks {
		new_seg.Blocks[k] = v.Copy()
	}

	return new_seg
}
