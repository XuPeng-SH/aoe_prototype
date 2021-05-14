package base

import (
	ops "aoe/pkg/engine/ops/base"
)

type IOpWorker interface {
	Start()
	Stop()
	SendOp(ops.IOp)
}
