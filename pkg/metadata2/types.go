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

type BoundSate uint8

const (
	STANDLONE BoundSate = iota
	Attached
	Detatched
)

type Block struct {
	BoundSate
	TimeStamp
	ID          uint64
	SegmentID   uint64
	BucketID    uint64
	PartitionID uint64
	TableID     uint64
	MaxRowCount uint64
	Count       uint64
	Index       *LogIndex
	PrevIndex   *LogIndex
	DeleteIndex *uint64
}

type Sequence struct {
	NextBlockID     uint64
	NextSegmentID   uint64
	NextBucketID    uint64
	NextPartitionID uint64
	NextTableID     uint64
}

type Segment struct {
	BoundSate
	sync.RWMutex
	TimeStamp
	ID            uint64
	BucketID      uint64
	PartitionID   uint64
	TableID       uint64
	MaxBlockCount uint64
	Blocks        map[uint64]*Block
}

type Bucket struct {
	BoundSate
	sync.RWMutex
	TimeStamp
	ID          uint64
	PartitionID uint64
	TableID     uint64
	Segments    map[uint64]*Segment
}

type Partition struct {
	BoundSate
	sync.RWMutex
	TimeStamp
	ID      uint64
	TableID uint64
	Buckets map[uint64]*Bucket
}

type Table struct {
	sync.RWMutex
	TimeStamp
	ID         uint64
	Partitions map[uint64]*Partition
}

type MetaInfo struct {
	sync.RWMutex
	Sequence Sequence
	Version  uint64
	Tables   map[uint64]*Table
}
