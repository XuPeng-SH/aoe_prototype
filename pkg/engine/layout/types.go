package layout

import (
	"sync/atomic"
)

type BlockId struct {
	TableID   uint64
	SegmentID uint64
	BlockID   uint64
	PartID    uint16
}

const (
	TRANSIENT_TABLE_START_ID uint64 = ^(uint64(0)) / 2
)

func NewTransientID() *BlockId {
	return &BlockId{
		TableID: TRANSIENT_TABLE_START_ID,
	}
}

func (id *BlockId) Next() *BlockId {
	new_id := atomic.AddUint64(&id.TableID, uint64(1))
	return &BlockId{
		TableID: new_id - 1,
	}
}

func (id *BlockId) IsTransient() bool {
	if id.TableID >= TRANSIENT_TABLE_START_ID {
		return true
	}
	return false
}
