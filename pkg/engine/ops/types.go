package ops

import (
	md "aoe/pkg/engine/metadata"
	iops "aoe/pkg/engine/ops/base"
	iworker "aoe/pkg/engine/worker/base"
	// log "github.com/sirupsen/logrus"
)

type OpCtx struct {
	TableID     uint64
	Block       *md.Block
	TmpMetaFile string
}

type Op struct {
	Ctx      *OpCtx
	MetaInfo *md.MetaInfo
	Impl     iops.IOpInternal
	ErrorC   chan error
	Worker   iworker.IOpWorker
	Err      error
	Result   interface{}
}
