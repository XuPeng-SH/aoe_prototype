package metadata

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
)

func (handle *BucketCacheHandle) GetVersion() uint64 {
	return handle.Cache.Version
}

func (handle *BucketCacheHandle) String() string {
	s := fmt.Sprintf("SS (V=%d) %s", handle.GetVersion(), handle.Cache.String())
	return s
}

func (handle *BucketCacheHandle) GetSegmentBlockIDs(segment_id uint64) map[uint64]ID {
	return handle.Cache.GetSegmentBlockIDs(segment_id)
}

func (handle *BucketCacheHandle) SegmentIDs() map[uint64]ID {
	return handle.Cache.SegmentIDs()
}

func (handle *BucketCacheHandle) Close() error {
	refs, succ := handle.Unref()
	if !succ {
		return errors.New("logic error closing snapshot")
	}
	if refs == 0 {
		log.Infof("BucketCache %d is ready for releasing.", handle.Cache.Version)
		if handle.OnNoRefFunc != nil {
			handle.OnNoRefFunc(handle)
		}
	}
	return nil
}

func (handle *BucketCacheHandle) NextBlock(segment_id uint64) (blk *Block, err error) {
	blk, err = handle.Cache.NewBlock(segment_id)
	return blk, err
}

func (handle *BucketCacheHandle) GetNextSegmentID() (id uint64, err error) {
	id, err = handle.Cache.GetNextSegmentID()
	return id, err
}

func (handle *BucketCacheHandle) GetSegment(id uint64) (seg *Segment, err error) {
	seg, err = handle.Cache.GetSegment(id)
	return seg, err
}

func (handle *BucketCacheHandle) GetNextBlockID(segment_id uint64) (id uint64, err error) {
	segment, err := handle.Cache.GetSegment(segment_id)
	if err != nil {
		return id, err
	}
	id = segment.GetNextBlockID()
	return id, err
}
