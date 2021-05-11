package ops

import (
	md "aoe/pkg/metadata"
	"errors"
	"fmt"
	// "fmt"
	// log "github.com/sirupsen/logrus"
)

type FlushOperation struct {
	Operation
}

func (op *FlushOperation) execute() error {
	latest_ss := md.CacheHolder.GetSnapshot()
	if latest_ss.Cache.CheckPoint.ID != op.Handle.Cache.CheckPoint.ID {
		return errors.New(fmt.Sprintf("Cannot flush. The expected CheckPoint is %s but actual is %s",
			op.Handle.Cache.CheckPoint.ID.String(), latest_ss.Cache.CheckPoint.ID.String()))
	}
	// ctx := md.CommitAddBlockContext{
	// 	Block:     op.Ctx.Block,
	// 	SegmentID: md.ID{ID: *op.Ctx.SegmentID},
	// }
	// latest_cache, err := latest_ss.Cache.CopyWithDelta(&ctx)
	// if err != nil {
	// 	return err
	// }

	// _, err = md.CacheHolder.Push(latest_cache)
	// if err != nil {
	// 	return err
	// }
	// op.LatestHandle = md.CacheHolder.GetSnapshot()
	return nil
}
