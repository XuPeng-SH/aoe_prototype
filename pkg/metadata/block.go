package metadata

import "fmt"

func NewBlock(bucket_id, segment_id, id uint64) *Block {
	blk := &Block{
		ID:        ID{ID: id},
		BucketID:  bucket_id,
		SegmentID: segment_id,
	}
	return blk
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
	return fmt.Sprintf("Blk(%d-%d-%s)", blk.BucketID, blk.SegmentID, blk.ID.String())
}
