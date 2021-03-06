package md2

import (
	"errors"
	"fmt"
)

const (
	BLOCK_ROW_COUNT = 16
)

func NewBlock(table_id, partition_id, bucket_id, segment_id, id uint64) *Block {
	blk := &Block{
		ID:          id,
		TableID:     table_id,
		PartitionID: partition_id,
		BucketID:    bucket_id,
		SegmentID:   segment_id,
		TimeStamp:   *NewTimeStamp(),
		MaxRowCount: BLOCK_ROW_COUNT,
	}
	return blk
}

func (blk *Block) GetAppliedIndex() (uint64, error) {
	if blk.DeleteIndex != nil {
		return *blk.DeleteIndex, nil
	}
	if blk.Index != nil && blk.Index.IsApplied() {
		return blk.Index.ID, nil
	}

	if blk.PrevIndex != nil {
		return blk.PrevIndex.ID, nil
	}

	return 0, errors.New("not applied")
}

func (blk *Block) GetID() uint64 {
	return blk.ID
}

func (blk *Block) GetSegmentID() uint64 {
	return blk.SegmentID
}

func (blk *Block) GetBucketID() uint64 {
	return blk.BucketID
}

func (blk *Block) SetIndex(idx LogIndex) {
	if blk.Index != nil {
		if !blk.Index.IsApplied() {
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

func (blk *Block) String() string {
	s := fmt.Sprintf("Blk(%d-%d-%d)[%s]", blk.BucketID, blk.SegmentID, blk.ID, blk.TimeStamp.String())
	if blk.IsDeleted() {
		s += "[D]"
	}
	return s
}

func (blk *Block) IsFull() bool {
	return blk.Count == blk.MaxRowCount
}

func (blk *Block) SetCount(count uint64) error {
	if count > blk.MaxRowCount {
		return errors.New("SetCount exceeds max limit")
	}
	if count <= blk.Count {
		return errors.New("SetCount cannot set smaller count")
	}
	blk.Count = count
	return nil
}
