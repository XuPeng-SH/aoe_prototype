package ops

import (
	md "aoe/pkg/metadata3"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
)

func NewUpdateOperation(ctx *OperationContext, info *md.MetaInfo,
	w IOpWorker) *UpdateOperation {
	op := &UpdateOperation{}
	op.Operation = *NewOperation(op, ctx, info, w)
	return op
}

type UpdateOperation struct {
	Operation
}

func (op *UpdateOperation) updateBlock(blk *md.Block) error {
	if blk.BoundSate != md.Detatched {
		log.Errorf("")
		return errors.New(fmt.Sprintf("Block %d BoundSate should be %d", blk.ID, md.Detatched))
	}

	table, err := op.MetaInfo.ReferenceTable(blk.TableID)
	if err != nil {
		return err
	}

	seg, err := table.ReferenceSegment(blk.SegmentID)
	if err != nil {
		return err
	}
	rblk, err := seg.ReferenceBlock(blk.ID)
	if err != nil {
		return err
	}
	err = rblk.Update(blk)
	if err != nil {
		return err
	}

	if rblk.IsFull() {
		seg.TryClose()
	}

	return nil
}

func (op *UpdateOperation) execute() error {
	if op.Ctx.Block != nil {
		return op.updateBlock(op.Ctx.Block)
	}
	return nil
}
