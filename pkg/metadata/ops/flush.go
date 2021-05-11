package ops

import (
	md "aoe/pkg/metadata"
	"errors"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

func NewFlushOperation(ctx *OperationContext, handle *md.BucketCacheHandle,
	w IOpWorker) *FlushOperation {
	op := &FlushOperation{}
	op.Operation = *NewOperation(op, ctx, handle, w)
	return op
}

type FlushOperation struct {
	Operation
}

func (op *FlushOperation) execute() error {
	latest_ss := md.CacheHolder.GetSnapshot()
	if latest_ss.Cache.CheckPoint != op.Handle.Cache.CheckPoint {
		return errors.New(fmt.Sprintf("Cannot flush. The expected CheckPoint is %s but actual is %s",
			op.Handle.Cache.CheckPoint.String(), latest_ss.Cache.CheckPoint.String()))
	}
	if op.Ctx.Block == nil {
		return errors.New("logic error")
	}
	ctx := md.CommitFlushBlockContext{
		Block: op.Ctx.Block,
	}
	new_cache, err := latest_ss.Cache.CopyWithFlush(&ctx)
	if err != nil {
		return err
	}

	err = new_cache.Serialize()
	if err != nil {
		return err
	}

	_, err = md.CacheHolder.Push(new_cache)
	if err != nil {
		return err
	}

	op.LatestHandle = md.CacheHolder.GetSnapshot()
	return nil
}
