package meta

import (
	// iops "aoe/pkg/engine/ops/base"
	"aoe/pkg/engine/ops"
	iworker "aoe/pkg/engine/worker/base"
	md "aoe/pkg/metadata3"
	// log "github.com/sirupsen/logrus"
)

func NewCreateBlockOperation(ctx *ops.OperationContext, info *md.MetaInfo,
	w iworker.IOpWorker) *CreateBlockOperation {
	op := &CreateBlockOperation{}
	op.Operation = *ops.NewOperation(op, ctx, info, w)
	return op
}

type CreateBlockOperation struct {
	ops.Operation
}

func (op *CreateBlockOperation) GetBlock() *md.Block {
	if op.Err != nil {
		return nil
	}
	return op.Result.(*md.Block)
}

func (op *CreateBlockOperation) Execute() error {
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
