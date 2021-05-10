package metadata

import (
	"errors"
	log "github.com/sirupsen/logrus"
)

func (handle *BucketCacheHandle) GetVersion() uint64 {
	return handle.Cache.Version
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

func (handle *BucketCacheHandle) GetNextBlockID(segment_id uint64) (id uint64, err error) {
	segment, err := handle.Cache.GetSegment(segment_id)
	if err != nil {
		return id, err
	}
	id = segment.GetNextBlockID()
	return id, err
}
