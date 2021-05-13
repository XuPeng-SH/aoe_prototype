package ops

// import (
// 	md "aoe/pkg/metadata"
// 	"errors"
// 	// "fmt"
// 	// log "github.com/sirupsen/logrus"
// )

// func NewUpdateBlockOperation(ctx *OperationContext, handle *md.BucketCacheHandle,
// 	w IOpWorker) *UpdateBlockOperation {
// 	op := &UpdateBlockOperation{}
// 	op.Operation = *NewOperation(op, ctx, handle, w)
// 	return op
// }

// type UpdateBlockOperation struct {
// 	Operation
// }

// func (op *UpdateBlockOperation) execute() error {
// 	if op.Ctx.Block == nil {
// 		return errors.New("No update block specified")
// 	}
// 	latest_ss := md.CacheHolder.GetSnapshot()

// 	seg, err := latest_ss.GetSegment(op.Ctx.Block.SegmentID)
// 	if err != nil {
// 		return err
// 	}

// 	blk := seg.GetBlock(op.Ctx.Block.ID.ID)
// 	if blk == nil {
// 		return errors.New("Specified update block not found")
// 	}

// 	ctx := md.CommitUpdateBlockContext{Block: op.Ctx.Block}
// 	latest_cache, err := latest_ss.Cache.CopyWithDelta(&ctx)
// 	if err != nil {
// 		return err
// 	}

// 	_, err = md.CacheHolder.Push(latest_cache)
// 	if err != nil {
// 		return err
// 	}
// 	op.LatestHandle = md.CacheHolder.GetSnapshot()
// 	return nil
// }
