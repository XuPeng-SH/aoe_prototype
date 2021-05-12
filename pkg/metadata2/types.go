package md2

import "sync"

type LogIndex struct {
	ID       uint64
	Start    uint64
	Count    uint64
	Capacity uint64
}

type TimeStamp struct {
	CreatedOn  int64
	UpdatedOn  int64
	DeltetedOn int64
}

type Block struct {
	TimeStamp
	ID          uint64
	SegmentID   uint64
	BucketID    uint64
	MaxRowCount uint64
	Count       uint64
	Index       *LogIndex
	PrevIndex   *LogIndex
	DeleteIndex *uint64
}

type Sequence struct {
	NextBlockID   uint64
	NextSegmentID uint64
	NextBucketUD  uint64
}

type Segment struct {
	sync.RWMutex
	TimeStamp
	ID            uint64
	BucketID      uint64
	MaxBlockCount uint64
	Blocks        map[uint64]*Block
}

type Bucket struct {
	TimeStamp
	ID uint64
}
