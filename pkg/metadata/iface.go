package metadata

import (
	"io"
)

type IRefProxy interface {
	Ref() bool
	Unref() (uint64, bool)
	HasRef() bool
}

type IBlock interface {
	IRefProxy
	GetID() MetaID
	GetCount() uint64
}

type ISegment interface {
	IRefProxy
	GetID() MetaID
	GetCount() uint64
	BlockCount() uint64
}

type IBucket interface {
	IRefProxy
	GetID() MetaID
	GetCount() uint64
	SegmentCount() uint64
}

type ISnapshotHandle interface {
	io.Closer
}

type ICacheBucket interface {
	GetSnapshot() ISnapshotHandle
}
