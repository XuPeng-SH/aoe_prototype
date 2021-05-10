package ops

import (
	md "aoe/pkg/metadata"
	"errors"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

type CreateBlockOperation struct {
	Operation
}

func (op *CreateBlockOperation) CommitNewBlock() (blk *md.Block, err error) {
	return blk, err
}

func (op *CreateBlockOperation) execute() error {
	if op.Ctx.Block == nil {
		return errors.New("logic error")
	}

	// TODO:
	if op.Ctx.CacheVersion != op.Handle.GetVersion() {
		return errors.New(fmt.Sprintf("CacheVersion %d mistach expect %d", op.Ctx.CacheVersion, op.Handle.GetVersion()))
	}

	latest_ss := md.CacheHolder.GetSnapshot()
	next_blk_id, err := latest_ss.GetNextBlockID(*op.Ctx.SegmentID)
	if err != nil {
		return err
	}
	if op.Ctx.Block.ID.ID != next_blk_id {
		return errors.New(fmt.Sprintf("Abort CreateBlockOperation due to race condition"))
	}

	return nil
}
