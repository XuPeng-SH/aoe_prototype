package metadata

import "sync"

type StateType uint32

type CallBackFunc func(interface{})

const (
	PENDING StateType = iota
	ACTIVE
	DEACTIVE
	COMMITTED
	DELETED
)

type LogIndex struct {
	ID       uint64
	Start    uint64
	Count    uint64
	Capacity uint64
}

type MetaID struct {
	ID   uint64
	Iter uint64
}

type State struct {
	Type StateType
}

type ID struct {
	ID MetaID
}

type TimeStamp struct {
	CreatedOn uint64
	UpdatedOn uint64
}

type RefProxy struct {
	Refs uint64
}

type Block struct {
	State
	ID
	TimeStamp
	// RefProxy
	Count uint64
}

type Segment struct {
	ID
	TimeStamp
	State
	// RefProxy
	Blocks []*Block
}

type Bucket struct {
	ID
	TimeStamp
	// RefProxy
	State
	Blocks []*Segment
}

type BucketCacheHandle struct {
	sync.Mutex
	Cache *BucketCache
	RefProxy
	OnNoRefFunc CallBackFunc
}

type BucketCache struct {
	CheckPoint *Bucket
	Delta      *Bucket
	Version    uint64
}

type BucketCacheHolder struct {
	sync.RWMutex
	Handle  *BucketCacheHandle
	Version uint64
}
