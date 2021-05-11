package metadata

import (
	"errors"
	"fmt"
)

func (cache *BucketCache) NextSegment() (seg *Segment, err error) {
	if cache.Delta != nil {
		seg, err = cache.Delta.NextSegment()
		return seg, err
	}
	seg, err = cache.CheckPoint.NextSegment()
	return seg, err
}

func (cache *BucketCache) NewBlock(segment_id uint64) (blk *Block, err error) {
	segment, err := cache.GetSegment(segment_id)
	if err != nil {
		blk = nil
		return blk, err
	}

	blk, err = segment.NextBlock()
	return blk, err
}

func (cache *BucketCache) SegmentIDs() map[uint64]ID {
	ids := make(map[uint64]ID, 0)
	if cache.CheckPoint != nil {
		ids = cache.CheckPoint.SegmentIDs()
	}
	if cache.Delta != nil {
		delta_ids := cache.Delta.SegmentIDs()
		for id, id_iter := range delta_ids {
			_, ok := ids[id]
			if !ok {
				ids[id] = id_iter
			}
		}
	}
	return ids
}

func (cache *BucketCache) GetNextSegmentID() (id uint64, err error) {
	if cache.Delta != nil {
		return cache.Delta.NextSegmentID, nil
	}

	return cache.CheckPoint.NextSegmentID, nil
}

func (cache *BucketCache) GetSegment(segment_id uint64) (seg *Segment, err error) {
	var ok bool
	if cache.Delta != nil {
		seg, ok = cache.Delta.GetSegment(segment_id)
	}
	if !ok {
		seg, ok = cache.CheckPoint.GetSegment(segment_id)
	}

	if !ok {
		return nil, errors.New(fmt.Sprintf("No specified segment %d", segment_id))
	}
	return seg, nil
}

func (cache *BucketCache) CopyWithDelta(ctx interface{}) (new_cache *BucketCache, err error) {
	new_delta := cache.Delta.Copy()
	new_cache = &BucketCache{
		CheckPoint: cache.CheckPoint,
		Version:    cache.Version + 1,
		Delta:      new_delta,
	}

	switch context := ctx.(type) {
	case *CommitAddBlockContext:
		new_cache.IncDeltaIter()
		segment, err := new_cache.GetSegment(context.SegmentID.ID)
		if err != nil {
			return nil, err
		}
		err = segment.AddBlock(context.Block)
		if err != nil {
			return nil, err
		}
		segment.IncIteration()
	case *CommitAddSegmentContext:
		new_cache.IncDeltaIter()
		err = new_cache.Delta.AddSegment(context.Segment)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("not support context")
	}
	return new_cache, err
}

func (cache *BucketCache) String() string {
	s := fmt.Sprintf("BCache (V%d) {", cache.Version)
	if cache.CheckPoint != nil {
		s += "\n\tCheckPoint: " + cache.CheckPoint.String()
	}
	if cache.Delta != nil {
		s += "\n\tDelta:      " + cache.Delta.String() + "\n"
	}
	s += "}"
	return s
}

// Modifier
func (cache *BucketCache) IncDeltaIter() error {
	if cache.Delta == nil {
		cache.Delta = NewBucket()
		cache.Delta.ID = cache.CheckPoint.ID
		cache.Delta.NextSegmentID = cache.CheckPoint.NextSegmentID
	}
	cache.Delta.IncIteration()
	return nil
}
