package metadata

import (
	"errors"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

func NewBucket() *Bucket {
	bucket := &Bucket{
		Segments: make(map[uint64]*Segment),
	}
	return bucket
}

func (bkt *Bucket) GetSegment(segment_id uint64) (seg *Segment, ok bool) {
	seg, ok = bkt.Segments[segment_id]
	return seg, ok
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
	s += "\n]"
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
	for k, v := range bkt.Segments {
		new_bkt.Segments[k] = v
	}

	return new_bkt
}
