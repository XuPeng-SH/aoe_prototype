package segment

import (
	"sync"
)

type ISegment interface {
	String() string
	ToString(verbose bool) string
	GetNext() ISegment
	Append(next ISegment)
	GetStartRow() uint64
	GetEndRow() uint64
	GetRowCount() uint64
	Capacity() uint64
}

const (
// SEGMENT_MAX_ROWS uint64 = constants.STANDARD_VECTOR_SIZE * MOSEL_VECTOR_COUNT
)

type Segment struct {
	StartRow uint64
	RowCount uint64
	Next     ISegment
}

type ISegmentTree interface {
	// All interfaces are not thread-safe. Should call RLock or Lock manually
	String() string
	ToString(depth uint64) string
	GetRoot() ISegment
	GetTail() ISegment
	Depth() uint64
	WhichSeg(row uint64) ISegment
	WhichSegIdx(row uint64) uint64
	Append(new_seg ISegment)
	// ReferenceOther(other ISegmentTree)
}

type SegmentTree struct {
	sync.RWMutex
	Segments []ISegment
}
