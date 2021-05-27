package coldata

import (
	e "aoe/pkg/engine"
	"aoe/pkg/engine/ops"
	iops "aoe/pkg/engine/ops/base"
	iworker "aoe/pkg/engine/worker/base"
	// log "github.com/sirupsen/logrus"
)

type OpCtx struct {
	Opts *e.Options
}

type Op struct {
	ops.Op
	Ctx *OpCtx
}

func NewOp(impl iops.IOpInternal, ctx *OpCtx, w iworker.IOpWorker) *Op {
	op := &Op{
		Ctx: ctx,
		Op: ops.Op{
			Impl:   impl,
			ErrorC: make(chan error),
			Worker: w,
		},
	}
	return op
}
