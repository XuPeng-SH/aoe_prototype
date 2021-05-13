package md2

import (
	"errors"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

func NewPartition(table_id uint64, ids ...uint64) *Partition {
	var id uint64
	if len(ids) == 0 {
		id = Meta.Sequence.GetPartitionID()
	} else {
		id = ids[0]
	}
	p := &Partition{
		ID:        id,
		TableID:   table_id,
		Buckets:   make(map[uint64]*Bucket),
		TimeStamp: *NewTimeStamp(),
	}
	return p
}

func (p *Partition) GetID() uint64 {
	return p.ID
}

func (p *Partition) ReferenceBucket(bucket_id uint64) (bkt *Bucket, err error) {
	p.RLock()
	defer p.RUnlock()
	bkt, ok := p.Buckets[bucket_id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("specified bucket %d not found in partition %d", bucket_id, p.ID))
	}
	return bkt, nil
}

func (p *Partition) BucketIDs(args ...int64) map[uint64]uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0]
	}
	ids := make(map[uint64]uint64)
	p.RLock()
	defer p.RUnlock()
	for _, bkt := range p.Buckets {
		if !bkt.Select(ts) {
			continue
		}
		ids[bkt.GetID()] = bkt.GetID()
	}
	return ids
}

func (p *Partition) CreateBucket() (bkt *Bucket, err error) {
	bkt = NewBucket(p.TableID, p.ID, Meta.Sequence.GetSegmentID())
	return bkt, err
}

func (p *Partition) String() string {
	s := fmt.Sprintf("Pat(%d-%d)", p.TableID, p.ID)
	s += "["
	for i, bkt := range p.Buckets {
		if i != 0 {
			s += "\n"
		}
		s += bkt.String()
	}
	if len(p.Buckets) > 0 {
		s += "\n"
	}
	s += "]"
	return s
}

func (p *Partition) RegisterBucket(bkt *Bucket) error {
	if p.ID != bkt.PartitionID {
		return errors.New(fmt.Sprintf("partition id mismatch %d:%d", p.ID, bkt.PartitionID))
	}
	if p.TableID != bkt.TableID {
		return errors.New(fmt.Sprintf("table id mismatch %d:%d", p.TableID, bkt.TableID))
	}
	p.Lock()
	defer p.Unlock()

	err := bkt.Attach()
	if err != nil {
		return err
	}

	_, ok := p.Buckets[bkt.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate bucket %d found in partition %d", bkt.ID, p.ID))
	}
	p.Buckets[bkt.ID] = bkt
	return nil
}

// // func (bkt *Bucket) Copy() *Bucket {
// // 	new_bkt := NewBucket()
// // 	new_bkt.ID = bkt.ID
// // 	new_bkt.TimeStamp = bkt.TimeStamp
// // 	new_bkt.State = bkt.State
// // 	new_bkt.NextSegmentID = bkt.NextSegmentID
// // 	for k, v := range bkt.Segments {
// // 		new_bkt.Segments[k] = v.Copy()
// // 	}

// // 	return new_bkt
// // }
