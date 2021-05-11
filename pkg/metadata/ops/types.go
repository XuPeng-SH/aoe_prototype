package ops

import (
	md "aoe/pkg/metadata"
	// "errors"
	// log "github.com/sirupsen/logrus"
)

type OperationContext struct {
	SegmentID    *uint64
	Block        *md.Block
	CacheVersion uint64
}

type Operation struct {
	Ctx          *OperationContext
	Handle       *md.BucketCacheHandle
	LatestHandle *md.BucketCacheHandle
}
