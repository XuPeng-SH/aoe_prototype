package metadata

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
)

const (
	BLOCK_ROW_COUNT = 16
)

func NewBlock(bucket_id, segment_id, id uint64) *Block {
	now := time.Now().Unix()
	blk := &Block{
		ID:          ID{ID: id},
		BucketID:    bucket_id,
		SegmentID:   segment_id,
		TimeStamp:   TimeStamp{CreatedOn: now, UpdatedOn: now},
		State:       State{Type: PENDING},
		MaxRowCount: BLOCK_ROW_COUNT,
	}
	return blk
}

func (blk *Block) SetCount(count uint64) error {
	if count > blk.MaxRowCount {
		return errors.New("SetCount exceeds max limit")
	}
	if count <= blk.Count {
		return errors.New("SetCount cannot set smaller count")
	}
	blk.Count = count
	if count == blk.MaxRowCount {
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

// After block is created, it is only updated during flusing
func (blk *Block) Update(other *Block) error {
	if blk.ID.ID != other.ID.ID || blk.SegmentID != other.SegmentID || blk.BucketID != blk.BucketID {
		return errors.New("Cannot merge blks with different IDs")
	}
	blk = other.Copy()
	return nil
}

func (blk *Block) SetIndex(idx LogIndex) {
	if blk.Index != nil {
		if !blk.Index.IsFull() {
			panic("logic error")
		}
		blk.PrevIndex = blk.Index
		blk.Index = &idx
	} else {
		if blk.PrevIndex != nil {
			panic("logic error")
		}
		blk.Index = &idx
	}
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
