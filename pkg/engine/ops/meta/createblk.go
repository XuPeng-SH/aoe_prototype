package meta

import (
	md "aoe/pkg/engine/metadata"
	"aoe/pkg/engine/ops"
	iworker "aoe/pkg/engine/worker/base"
	// log "github.com/sirupsen/logrus"
)

func NewCreateBlockOperation(ctx *ops.OpCtx, info *md.MetaInfo,
	w iworker.IOpWorker) *CreateBlkOp {
	op := &CreateBlkOp{}
	op.Op = *ops.NewOp(op, ctx, info, w)
	return op
}

type CreateBlkOp struct {
	ops.Op
}

func (op *CreateBlkOp) GetBlock() *md.Block {
	if op.Err != nil {
		return nil
	}
	return op.Result.(*md.Block)
}

func (op *CreateBlkOp) Execute() error {
	table, err := op.MetaInfo.ReferenceTable(op.Ctx.TableID)
	if err != nil {
		return err
	}

	seg, err := table.GetInfullSegment()
	if err != nil {
		seg, err = table.CreateSegment()
		if err != nil {
			return err
		}
		err = table.RegisterSegment(seg)
		if err != nil {
			return err
		}
	}
	blk, err := seg.CreateBlock()
	if err != nil {
		return err
	}
	err = seg.RegisterBlock(blk)
	if err != nil {
		return err
	}
	cloned, err := seg.CloneBlock(blk.ID)
	if err != nil {
		return err
	}
	op.Result = cloned
	return err
}
