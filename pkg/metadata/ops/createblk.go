package ops

import (
	md "aoe/pkg/metadata"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
)

func NewCreateBlockOperation(ctx *OperationContext, handle *md.BucketCacheHandle,
	w IOpWorker) *CreateBlockOperation {
	op := &CreateBlockOperation{}
	op.Operation = *NewOperation(op, ctx, handle, w)
	return op
}

type CreateBlockOperation struct {
	Operation
}

func (op *CreateBlockOperation) CommitNewBlock(segment_id uint64) (blk *md.Block, err error) {
	blk, err = op.Handle.Cache.NewBlock(segment_id)
	if err == nil {
		op.Ctx.Block = blk
	}
	return blk, err
}

func (op *CreateBlockOperation) execute() error {
	if op.Ctx.Block == nil {
		return errors.New("No committed new block")
	}

	// TODO:
	if op.Ctx.CacheVersion != op.Handle.GetVersion() {
		msg := fmt.Sprintf("CacheVersion %d mistach expect %d", op.Ctx.CacheVersion, op.Handle.GetVersion())
		log.Errorf(msg)
		return errors.New(msg)
	}

	latest_ss := md.CacheHolder.GetSnapshot()
	next_blk_id, err := latest_ss.GetNextBlockID(op.Ctx.Block.SegmentID)
	if err != nil {
		return err
	}
	if op.Ctx.Block.ID.ID != next_blk_id {
		return errors.New(fmt.Sprintf("Abort CreateBlockOperation due to race condition"))
	}
	ctx := md.CommitAddBlockContext{
		Block: op.Ctx.Block,
	}
	latest_cache, err := latest_ss.Cache.CopyWithDelta(&ctx)
	if err != nil {
		return err
	}

	_, err = md.CacheHolder.Push(latest_cache)
	if err != nil {
		return err
	}
	op.LatestHandle = md.CacheHolder.GetSnapshot()
	return nil
}
