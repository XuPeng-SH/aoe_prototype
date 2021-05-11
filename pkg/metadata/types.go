package metadata

import "sync"

type StateType = uint32

type CallBackFunc func(interface{})

const (
	PENDING StateType = iota
	DROPPENDING
	COMMITTED
	DROPCOMMITTED
)

type IndexType = uint32

const (
	ZONEMAP IndexType = iota
)

type LogIndex struct {
	ID       uint64
	Start    uint64
	Count    uint64
	Capacity uint64
}

type ID struct {
	ID   uint64
	Iter uint64
}

type State struct {
	Type StateType
}

type TimeStamp struct {
	CreatedOn int64
	UpdatedOn int64
}

type RefProxy struct {
	Refs uint64
}

type Index struct {
	State
	ID
	TimeStamp
	SegmentID uint64
	BucketID  uint64
}

type Block struct {
	State
	ID
	TimeStamp
	SegmentID uint64
	BucketID  uint64
	Count     uint64
}

type Segment struct {
	ID
	TimeStamp
	State
	BucketID    uint64
	Blocks      map[uint64]*Block
	NextBlockID uint64
}

type Bucket struct {
	ID
	TimeStamp
	State
	Segments      map[uint64]*Segment
	NextSegmentID uint64
}

type BucketCacheHandle struct {
	sync.Mutex
	Cache *BucketCache
	RefProxy
	OnNoRefFunc CallBackFunc
}

type BucketCache struct {
	CheckPoint ID
	Delta      *Bucket
	Version    uint64
}

// type BucketPersistentContext struct {
// 	Bucket *Bucket
// }

// type BucketTransientContext struct {
// 	Segment *Segment
// 	Block   *Block
// }

type BucketCacheHolderContext struct {
	// NewBlock *Block
	// Handle *BucketCacheHandle
	// PersistentCtx *BucketPersistentContext
	// TransientCtx  *BucketTransientContext
}

type BucketCacheHolder struct {
	sync.RWMutex
	Handle  *BucketCacheHandle
	Version uint64
}

var (
	CacheHolder *BucketCacheHolder
)

func init() {
	CacheHolder = NewCacheHolder()
}

type CommitAddBlockContext struct {
	Block *Block
}

type CommitFlushBlockContext struct {
	Block *Block
}

type CommitAddSegmentContext struct {
	Segment *Segment
}
