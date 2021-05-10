package metadata

import (
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
