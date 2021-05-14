package meta

import (
	md "aoe/pkg/engine/metadata"
	"aoe/pkg/engine/ops"
	iops "aoe/pkg/engine/ops/base"
	iworker "aoe/pkg/engine/worker/base"
	// log "github.com/sirupsen/logrus"
)

func NewOp(impl iops.IOpInternal, ctx *OpCtx,
	info *md.MetaInfo, w iworker.IOpWorker) *Op {
	op := &Op{
		Ctx:      ctx,
		MetaInfo: info,
		Op: ops.Op{
			Impl:   impl,
			ErrorC: make(chan error),
			Worker: w,
		},
	}
	return op
}
