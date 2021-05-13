package md2

import (
	"errors"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

func NewBucket(ids ...uint64) *Bucket {
	var id uint64
	if len(ids) == 0 {
		id = SEQUENCE.NextBucketID
	} else {
		id = ids[0]
	}
	bkt := &Bucket{
		ID:        id,
		Segments:  make(map[uint64]*Segment),
		TimeStamp: *NewTimeStamp(),
	}
	return bkt
}

func (bkt *Bucket) ReferenceSegment(segment_id uint64) (seg *Segment, err error) {
	bkt.RLock()
	defer bkt.RUnlock()
	seg, err = bkt.referenceSegmentNoLock(segment_id)
	return seg, err
}

func (bkt *Bucket) referenceSegmentNoLock(segment_id uint64) (seg *Segment, err error) {
	seg, ok := bkt.Segments[segment_id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("specified segment %d not found in bucket %d", segment_id, bkt.ID))
	}
	return seg, nil
}

func (bkt *Bucket) GetSegmentBlockIDs(segment_id uint64, args ...int64) map[uint64]uint64 {
	bkt.RLock()
	seg, err := bkt.referenceSegmentNoLock(segment_id)
	bkt.RUnlock()
	if err == nil {
		return make(map[uint64]uint64, 0)
	}
	return seg.BlockIDs(args)
}

func (bkt *Bucket) SegmentIDs(args ...int64) map[uint64]uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0]
	}
	ids := make(map[uint64]uint64)
	bkt.RLock()
	defer bkt.RUnlock()
	for _, seg := range bkt.Segments {
		if !seg.Select(ts) {
			continue
		}
		ids[seg.ID] = seg.ID
	}
	return ids
}

func (bkt *Bucket) CreateSegment() (seg *Segment, err error) {
	seg = NewSegment(bkt.ID, SEQUENCE.GetSegmentID())
	return seg, err
}

func (bkt *Bucket) String() string {
	s := fmt.Sprintf("Buk(%d)", bkt.ID)
	s += "["
	for i, seg := range bkt.Segments {
		if i != 0 {
			s += "\n"
		}
		s += seg.String()
	}
	if len(bkt.Segments) > 0 {
		s += "\n"
	}
	s += "]"
	return s
}

func (bkt *Bucket) RegisterSegment(seg *Segment) error {
	if bkt.ID != seg.GetBucketID() {
		return errors.New(fmt.Sprintf("bucket id mismatch %d:%d", bkt.ID, seg.GetBucketID()))
	}
	bkt.Lock()
	defer bkt.Unlock()

	err := seg.Attach()
	if err != nil {
		return err
	}

	_, ok := bkt.Segments[seg.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate segment %d found in bucket %d", seg.GetID(), bkt.ID))
	}
	bkt.Segments[seg.GetID()] = seg
	return nil
}

// func (bkt *Bucket) Copy() *Bucket {
// 	new_bkt := NewBucket()
// 	new_bkt.ID = bkt.ID
// 	new_bkt.TimeStamp = bkt.TimeStamp
// 	new_bkt.State = bkt.State
// 	new_bkt.NextSegmentID = bkt.NextSegmentID
// 	for k, v := range bkt.Segments {
// 		new_bkt.Segments[k] = v.Copy()
// 	}

// 	return new_bkt
// }
