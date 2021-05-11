package metadata

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
	// log "github.com/sirupsen/logrus"
)

func NewBucket() *Bucket {
	now := time.Now().Unix()
	bucket := &Bucket{
		Segments:  make(map[uint64]*Segment),
		TimeStamp: TimeStamp{CreatedOn: now, UpdatedOn: now},
		State:     State{Type: PENDING},
	}
	return bucket
}

func (bkt *Bucket) GetSegment(segment_id uint64) (seg *Segment, ok bool) {
	seg, ok = bkt.Segments[segment_id]
	return seg, ok
}

func (bkt *Bucket) GetSegmentBlockIDs(segment_id uint64) map[uint64]ID {
	seg, ok := bkt.GetSegment(segment_id)
	if !ok {
		return make(map[uint64]ID, 0)
	}
	return seg.BlockIDs()
}

func (bkt *Bucket) SegmentIDs() map[uint64]ID {
	ids := make(map[uint64]ID)
	for _, seg := range bkt.Segments {
		ids[seg.ID.ID] = seg.ID
	}
	return ids
}

func (bkt *Bucket) NextSegment() (seg *Segment, err error) {
	seg_id := atomic.LoadUint64(&(bkt.NextSegmentID))
	seg = NewSegment(bkt.ID.ID, seg_id)
	return seg, err
}

func (bkt *Bucket) String() string {
	s := fmt.Sprintf("Buk(%s,NSeg=%d)", bkt.ID.String(), bkt.NextSegmentID)
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

func (bkt *Bucket) AddSegment(seg *Segment) error {
	if bkt.NextSegmentID != seg.ID.ID {
		return errors.New(fmt.Sprintf("AddSegment %d is mismatch with NextSegmentID %d", seg.ID.ID, bkt.NextSegmentID))
	}
	bkt.Segments[seg.ID.ID] = seg
	bkt.NextSegmentID += 1
	return nil
}

func (bkt *Bucket) Copy() *Bucket {
	new_bkt := NewBucket()
	new_bkt.ID = bkt.ID
	new_bkt.TimeStamp = bkt.TimeStamp
	new_bkt.State = bkt.State
	new_bkt.NextSegmentID = bkt.NextSegmentID
	for k, v := range bkt.Segments {
		new_bkt.Segments[k] = v.Copy()
	}

	return new_bkt
}
