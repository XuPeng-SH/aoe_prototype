package metadata

import (
	"errors"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

func NewCacheHolder() *BucketCacheHolder {
	holder := &BucketCacheHolder{
		// Snapshots: make(map[uint64]*BucketCacheHandle),
	}
	cache := &BucketCache{
		CheckPoint: ID{},
		Delta:      &Bucket{},
	}
	holder.Push(cache)
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
	if holder.Handle == nil {
		return nil
	}
	holder.Handle.Ref()
	return holder.Handle
}

func (holder *BucketCacheHolder) NextVersion() uint64 {
	return atomic.LoadUint64(&(holder.Version))
}

// func (holder *BucketCacheHolder) Next(ctx *BucketCacheHolderContext) error {
// 	holder.RLock()
// 	defer holder.Unlock()
// 	// Build cache base from ctx and holder.cache
// 	// Push(cache)
// 	return nil
// }
