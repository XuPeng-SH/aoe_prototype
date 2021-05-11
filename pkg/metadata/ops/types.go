package ops

import (
	md "aoe/pkg/metadata"
	// "errors"
	// log "github.com/sirupsen/logrus"
)

type OperationContext struct {
	SegmentID    *uint64
	Block        *md.Block
	Segment      *md.Segment
	CacheVersion uint64
}

type IOperation interface {
	preExecute() error
	execute() error
	postExecute() error
}

type Operation struct {
	Ctx          *OperationContext
	Handle       *md.BucketCacheHandle
	LatestHandle *md.BucketCacheHandle
	Impl         IOperation
}
