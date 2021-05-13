package md3

import (
	"errors"
	"fmt"
)

const (
	SEGMENT_BLOCK_COUNT = 4
)

func NewSegment(table_id, id uint64) *Segment {
	seg := &Segment{
		ID:            id,
		TableID:       table_id,
		Blocks:        make(map[uint64]*Block),
		TimeStamp:     *NewTimeStamp(),
		MaxBlockCount: SEGMENT_BLOCK_COUNT,
	}
	return seg
}

func (seg *Segment) GetTableID() uint64 {
	return seg.TableID
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
	blk = NewBlock(seg.TableID, seg.ID, Meta.Sequence.GetBlockID())
	return blk, err
}

func (seg *Segment) String() string {
	s := fmt.Sprintf("Seg(%d-%d)", seg.TableID, seg.ID)
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

func (seg *Segment) ReferenceBlock(id uint64) (blk *Block, err error) {
	seg.RLock()
	defer seg.RUnlock()
	blk, ok := seg.Blocks[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("block %d not found in segment %d", id, seg.ID))
	}
	return blk, nil
}

func (seg *Segment) RegisterBlock(blk *Block) error {
	if blk.TableID != seg.TableID {
		return errors.New(fmt.Sprintf("table id mismatch %d:%d", seg.TableID, blk.TableID))
	}
	if blk.GetSegmentID() != seg.GetID() {
		return errors.New(fmt.Sprintf("segment id mismatch %d:%d", seg.GetID(), blk.GetSegmentID()))
	}
	seg.Lock()
	defer seg.Unlock()

	err := blk.Attach()
	if err != nil {
		return err
	}
	if len(seg.Blocks) == int(seg.MaxBlockCount) {
		return errors.New(fmt.Sprintf("Cannot add block into full segment %d", seg.ID))
	}
	_, ok := seg.Blocks[blk.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate block %d found in segment %d", blk.GetID(), seg.ID))
	}
	seg.Blocks[blk.GetID()] = blk
	return nil
}
