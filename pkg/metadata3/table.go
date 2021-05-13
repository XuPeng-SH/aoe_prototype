package md3

import (
	"errors"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

func NewTable(ids ...uint64) *Table {
	var id uint64
	if len(ids) == 0 {
		id = Meta.Sequence.GetTableID()
	} else {
		id = ids[0]
	}
	tbl := &Table{
		ID:        id,
		Segments:  make(map[uint64]*Segment),
		TimeStamp: *NewTimeStamp(),
	}
	return tbl
}

func (tbl *Table) GetID() uint64 {
	return tbl.ID
}

func (tbl *Table) CloneSegment(segment_id uint64) (seg *Segment, err error) {
	tbl.RLock()
	defer tbl.RUnlock()
	seg, err = tbl.referenceSegmentNoLock(segment_id)
	if err != nil {
		return nil, err
	}
	seg = seg.Copy()
	err = seg.Detach()
	return seg, err
}

func (tbl *Table) ReferenceBlock(segment_id, block_id uint64) (blk *Block, err error) {
	tbl.RLock()
	seg, err := tbl.referenceSegmentNoLock(segment_id)
	if err != nil {
		tbl.RUnlock()
		return nil, err
	}
	tbl.RUnlock()

	blk, err = seg.ReferenceBlock(block_id)

	return blk, err
}

func (tbl *Table) ReferenceSegment(segment_id uint64) (seg *Segment, err error) {
	tbl.RLock()
	defer tbl.RUnlock()
	seg, err = tbl.referenceSegmentNoLock(segment_id)
	return seg, err
}

func (tbl *Table) referenceSegmentNoLock(segment_id uint64) (seg *Segment, err error) {
	seg, ok := tbl.Segments[segment_id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("specified segment %d not found in table %d", segment_id, tbl.ID))
	}
	return seg, nil
}

func (tbl *Table) GetSegmentBlockIDs(segment_id uint64, args ...int64) map[uint64]uint64 {
	tbl.RLock()
	seg, err := tbl.referenceSegmentNoLock(segment_id)
	tbl.RUnlock()
	if err == nil {
		return make(map[uint64]uint64, 0)
	}
	return seg.BlockIDs(args)
}

func (tbl *Table) SegmentIDs(args ...int64) map[uint64]uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0]
	}
	ids := make(map[uint64]uint64)
	tbl.RLock()
	defer tbl.RUnlock()
	for _, seg := range tbl.Segments {
		if !seg.Select(ts) {
			continue
		}
		ids[seg.ID] = seg.ID
	}
	return ids
}

func (tbl *Table) CreateSegment() (seg *Segment, err error) {
	seg = NewSegment(tbl.ID, Meta.Sequence.GetSegmentID())
	return seg, err
}

func (tbl *Table) GetInfullSegment() (seg *Segment, err error) {
	tbl.RLock()
	defer tbl.RUnlock()
	for _, seg := range tbl.Segments {
		if seg.DataState == EMPTY || seg.DataState == PARTIAL {
			return seg, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("no infull segment found in table %d", tbl.ID))
}

func (tbl *Table) String() string {
	s := fmt.Sprintf("Tbl(%d)", tbl.ID)
	s += "["
	for i, seg := range tbl.Segments {
		if i != 0 {
			s += "\n"
		}
		s += seg.String()
	}
	if len(tbl.Segments) > 0 {
		s += "\n"
	}
	s += "]"
	return s
}

func (tbl *Table) RegisterSegment(seg *Segment) error {
	if tbl.ID != seg.GetTableID() {
		return errors.New(fmt.Sprintf("table id mismatch %d:%d", tbl.ID, seg.GetTableID()))
	}
	tbl.Lock()
	defer tbl.Unlock()

	err := seg.Attach()
	if err != nil {
		return err
	}

	_, ok := tbl.Segments[seg.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate segment %d found in table %d", seg.GetID(), tbl.ID))
	}
	tbl.Segments[seg.GetID()] = seg
	return nil
}
