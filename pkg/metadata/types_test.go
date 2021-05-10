package metadata

import (
	"github.com/stretchr/testify/assert"
	"testing"
	// "time"
)

func TestCache(t *testing.T) {
	// blk := Block{}
	// assert.False(t, blk.HasRef())
	// curr, succ := blk.Unref()
	// assert.Equal(t, curr, uint64(0))
	// assert.False(t, succ)
	// blk.Ref()
	// assert.True(t, blk.HasRef())
	// curr, succ = blk.Unref()
	// assert.Equal(t, curr, uint64(0))
	// assert.True(t, succ)
	// curr, succ = blk.Unref()
	// assert.Equal(t, curr, uint64(0))
	// assert.False(t, succ)
	holder := NewCacheHolder()
	cache := BucketCache{
		CheckPoint: &Bucket{},
		Delta:      &Bucket{},
	}
	version, err := holder.Push(&cache)
	assert.Nil(t, err)
	assert.Equal(t, version, uint64(0))
	assert.True(t, holder.Handle.HasRef())
	assert.Equal(t, holder.Handle.Refs, uint64(1))

	{
		handle := holder.GetSnapshot()
		assert.True(t, handle.HasRef())
		assert.Equal(t, handle.Refs, uint64(2))
		handle.Close()
	}
	assert.True(t, holder.Handle.HasRef())
	assert.Equal(t, holder.Handle.Refs, uint64(1))

	cache2 := BucketCache{
		CheckPoint: &Bucket{},
		Delta:      &Bucket{},
	}

	version, err = holder.Push(&cache2)
	assert.Nil(t, err)
	assert.Equal(t, version, uint64(1))

	{
		handle := holder.GetSnapshot()
		assert.True(t, handle.HasRef())
		assert.Equal(t, handle.Refs, uint64(2))
		handle.Close()
	}

	// time.Sleep(time.Duration(1) * time.Second)
}
