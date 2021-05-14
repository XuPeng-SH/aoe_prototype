package data

import (
	imem "aoe/pkg/engine/memtable/base"
	iworker "aoe/pkg/engine/worker/base"
	log "github.com/sirupsen/logrus"
)

func NewFlushBlkOp(ctx *OpCtx,
	w iworker.IOpWorker) *FlushBlkOp {
	op := &FlushBlkOp{}
	op.Op = *NewOp(op, ctx, w)
	return op
}

type FlushBlkOp struct {
	Op
}

func (op *FlushBlkOp) onFlushErr(mem imem.IMemTable) {
}

// This Op is create when a memtable is full, and it is sent to meta Flusher queue.
// The Flusher executes this op.
func (op *FlushBlkOp) Execute() error {
	var mem imem.IMemTable
	if op.Ctx.MemTable != nil {
		mem = op.Ctx.MemTable
	} else if op.Ctx.Collection != nil {
		mem = op.Ctx.Collection.FetchImmuTable()
		if mem == nil {
			return nil
		}
	} else {
		return nil
	}
	err := mem.Flush()
	if err != nil {
		op.onFlushErr(mem)
		log.Errorf("Flush memtable %d failed %s", mem.GetMeta().GetID(), err)
		return err
	}

	return nil
}
