package ops

import (
	md "aoe/pkg/engine/metadata"
	iops "aoe/pkg/engine/ops/base"
	iworker "aoe/pkg/engine/worker/base"
	// log "github.com/sirupsen/logrus"
)

type OperationContext struct {
	TableID     uint64
	Block       *md.Block
	TmpMetaFile string
}

type Operation struct {
	Ctx      *OperationContext
	MetaInfo *md.MetaInfo
	Impl     iops.IOperationInternal
	ErrorC   chan error
	Worker   iworker.IOpWorker
	Err      error
	Result   interface{}
}
