package metadata

import (
	"errors"
	"fmt"
	"time"
)

const (
	BLOCK_ROW_COUNT = 16
)

func NewBlock(bucket_id, segment_id, id uint64) *Block {
	now := time.Now().Unix()
	blk := &Block{
		ID:        ID{ID: id},
		BucketID:  bucket_id,
		SegmentID: segment_id,
		TimeStamp: TimeStamp{CreatedOn: now, UpdatedOn: now},
		State:     State{Type: PENDING},
	}
	return blk
}

func (blk *Block) SetCount(count uint64) error {
	if count > BLOCK_ROW_COUNT {
		return errors.New("SetCount exceeds max limit")
	}
	if count <= blk.Count {
		return errors.New("SetCount cannot set smaller count")
	}
	blk.Count = count
	if count == BLOCK_ROW_COUNT {
		blk.DataState = FULL
	} else {
		blk.DataState = PARTIAL
	}
	return nil
}

func (blk *Block) IsActive() bool {
	if blk.DataState == EMPTY || blk.DataState == PARTIAL {
		return true
	}
	return false
}

func (blk *Block) GetID() ID {
	return blk.ID
}

func (blk *Block) GetSegmentID() uint64 {
	return blk.SegmentID
}

func (blk *Block) GetBucketID() uint64 {
	return blk.BucketID
}

func (blk *Block) String() string {
	return fmt.Sprintf("Blk[%s-%s](%d-%d-%s)", blk.State.String(), ToString(blk.DataState), blk.BucketID, blk.SegmentID, blk.ID.String())
}

func (blk *Block) Copy() *Block {
	new_blk := NewBlock(blk.BucketID, blk.SegmentID, blk.ID.ID)
	new_blk.ID = blk.ID
	new_blk.TimeStamp = blk.TimeStamp
	new_blk.State = blk.State
	new_blk.Count = blk.Count
	new_blk.DataState = blk.DataState

	return new_blk
}
