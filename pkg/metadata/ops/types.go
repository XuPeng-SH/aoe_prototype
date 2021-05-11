package ops

import (
	md "aoe/pkg/metadata"
	// "errors"
	// log "github.com/sirupsen/logrus"
)

type OperationContext struct {
	Block        *md.Block
	Segment      *md.Segment
	CacheVersion uint64
	TmpMetaFile  string
}

type IOperationInternal interface {
	preExecute() error
	execute() error
	postExecute() error
}

type IOperation interface {
	OnExecute() error
	SetError(err error)
}

type Operation struct {
	Ctx          *OperationContext
	Handle       *md.BucketCacheHandle
	LatestHandle *md.BucketCacheHandle
	Impl         IOperationInternal
	ResultC      chan error
	Worker       IOpWorker
}
