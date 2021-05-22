package layout

import (
	"sync/atomic"
)

type BlockId struct {
	TableID   uint64
	SegmentID uint64
	BlockID   uint64
	PartID    uint32
}

const (
	TRANSIENT_TABLE_START_ID uint64 = ^(uint64(0)) / 2
)

func NewTransientID() *BlockId {
	return &BlockId{
		TableID: TRANSIENT_TABLE_START_ID,
	}
}

func (id *BlockId) IsSameSegment(o BlockId) bool {
	return id.TableID == o.TableID && id.SegmentID == o.SegmentID
}

func (id *BlockId) IsSameBlock(o BlockId) bool {
	return id.TableID == o.TableID && id.SegmentID == o.SegmentID && id.BlockID == o.BlockID
}

func (id *BlockId) Next() *BlockId {
	new_id := atomic.AddUint64(&id.TableID, uint64(1))
	return &BlockId{
		TableID: new_id - 1,
	}
}

func (id *BlockId) NextPart() BlockId {
	new_id := atomic.AddUint32(&id.PartID, uint32(1))
	bid := *id
	bid.PartID = new_id - 1
	return bid
}

func (id *BlockId) NextBlock() BlockId {
	new_id := atomic.AddUint64(&id.BlockID, uint64(1))
	bid := *id
	bid.BlockID = new_id - 1
	return bid
}

func (id *BlockId) NextSegment() BlockId {
	new_id := atomic.AddUint64(&id.SegmentID, uint64(1))
	bid := *id
	bid.SegmentID = new_id - 1
	return bid
}

func (id *BlockId) IsTransient() bool {
	if id.TableID >= TRANSIENT_TABLE_START_ID {
		return true
	}
	return false
}
