package metadata

import (
	"errors"
	log "github.com/sirupsen/logrus"
)

func NewCacheHolder() *BucketCacheHolder {
	holder := &BucketCacheHolder{
		// Snapshots: make(map[uint64]*BucketCacheHandle),
	}
	return holder
}

func (holder *BucketCacheHolder) Push(cache *BucketCache) (uint64, error) {
	if cache == nil {
		log.Error("logic error")
		return cache.Version, errors.New("logic error")
	}
	holder.Lock()
	defer holder.Unlock()
	stale := holder.Handle
	cache.Version = holder.Version
	holder.Version++
	holder.Handle = &BucketCacheHandle{
		Cache: cache,
	}
	holder.Handle.Ref()
	go func() {
		if stale != nil {
			stale.Close()
		}
	}()
	return cache.Version, nil
}

func (holder *BucketCacheHolder) GetSnapshot() *BucketCacheHandle {
	holder.RLock()
	defer holder.RUnlock()
	holder.Handle.Ref()
	return holder.Handle
}
