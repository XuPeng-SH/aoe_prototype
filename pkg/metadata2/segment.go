package md2

import (
	"errors"
	"fmt"
)

const (
	SEGMENT_BLOCK_COUNT = 4
)

func NewSegment(bucket_id, id uint64) *Segment {
	seg := &Segment{
		ID:            id,
		BucketID:      bucket_id,
		Blocks:        make(map[uint64]*Block),
		TimeStamp:     *NewTimeStamp(),
		MaxBlockCount: SEGMENT_BLOCK_COUNT,
	}
	return seg
}

func (seg *Segment) GetBucketID() uint64 {
	return seg.BucketID
}

func (seg *Segment) GetID() uint64 {
	return seg.ID
}

func (seg *Segment) BlockIDs(args ...interface{}) map[uint64]uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0].(int64)
	}
	ids := make(map[uint64]uint64)
	seg.RLock()
	defer seg.RUnlock()
	for _, blk := range seg.Blocks {
		if !blk.Select(ts) {
			continue
		}
		ids[blk.ID] = blk.ID
	}
	return ids
}

func (seg *Segment) CreateBlock() (blk *Block, err error) {
	blk = NewBlock(seg.BucketID, seg.ID, SEQUENCE.GetBlockID())
	return blk, err
}

// func (seg *Segment) GetActiveBlock() (*Block, error) {
// 	if !seg.IsActive() {
// 		return nil, errors.New("segment is closed")
// 	}
// 	min_blk_id := seg.NextBlockID
// 	for blk_id, itblk := range seg.Blocks {
// 		if blk_id < min_blk_id && itblk.IsActive() {
// 			min_blk_id = blk_id
// 		}
// 	}
// 	if min_blk_id == seg.NextBlockID {
// 		// Need create new block for this segment
// 		return nil, nil
// 	}
// 	return seg.Blocks[min_blk_id], nil
// }

// func (seg *Segment) IsActive() bool {
// 	if seg.DataState == EMPTY || seg.DataState == PARTIAL {
// 		return true
// 	}
// 	return false
// }

func (seg *Segment) String() string {
	s := fmt.Sprintf("Seg(%d-%d)", seg.BucketID, seg.ID)
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

func (seg *Segment) GetBlock(id uint64) (blk *Block, err error) {
	seg.RLock()
	defer seg.RUnlock()
	blk, ok := seg.Blocks[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("block %d not found in segment %d", id, seg.ID))
	}
	return blk, nil
}

// func (seg *Segment) UpdateBlock(blk *Block) (*Block, error) {
// 	target_blk := seg.GetBlock(blk.ID.ID)
// 	if target_blk == nil {
// 		return nil, errors.New("Update block not found")
// 	}
// 	if target_blk.DataState >= blk.DataState {
// 		return nil, errors.New("Cannot update with higher DataState")
// 	}
// 	seg.Blocks[blk.ID.ID] = blk.Copy()
// 	if !blk.IsActive() {
// 		full_blocks := 0
// 		for _, itblk := range seg.Blocks {
// 			if !itblk.IsActive() {
// 				full_blocks++
// 			}
// 		}
// 		if full_blocks < int(seg.MaxBlockCount) {
// 			seg.DataState = PARTIAL
// 		} else {
// 			seg.DataState = FULL
// 		}
// 	}
// 	return seg.Blocks[blk.ID.ID], nil
// }

func (seg *Segment) RegisterBlock(blk *Block) error {
	seg.Lock()
	defer seg.Unlock()
	if blk.GetBucketID() != seg.GetBucketID() {
		return errors.New(fmt.Sprintf("bucket id mismatch %d:%d", seg.GetBucketID(), blk.GetSegmentID()))
	}
	if blk.GetSegmentID() != seg.GetID() {
		return errors.New(fmt.Sprintf("segment id mismatch %d:%d", seg.GetID(), blk.GetSegmentID()))
	}
	if len(seg.Blocks) == int(seg.MaxBlockCount) {
		return errors.New(fmt.Sprintf("Cannot add block into full segment %d", seg.ID))
	}
	seg.Blocks[blk.ID] = blk
	return nil
}

// func (seg *Segment) Copy() *Segment {
// 	new_seg := NewSegment(seg.BucketID, seg.ID.ID)
// 	new_seg.ID = seg.ID
// 	new_seg.TimeStamp = seg.TimeStamp
// 	new_seg.State = seg.State
// 	new_seg.DataState = seg.DataState
// 	new_seg.NextBlockID = seg.NextBlockID
// 	for k, v := range seg.Blocks {
// 		new_seg.Blocks[k] = v.Copy()
// 	}

// 	return new_seg
// }
