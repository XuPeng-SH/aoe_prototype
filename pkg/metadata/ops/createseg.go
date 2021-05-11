package ops

import (
	md "aoe/pkg/metadata"
	"errors"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

type CreateSegmentOperation struct {
	Operation
}

func (op *CreateSegmentOperation) CommitNewSegment() (seg *md.Segment, err error) {
	return seg, err
}

func (op *CreateSegmentOperation) execute() error {
	if op.Ctx.Segment == nil {
		return errors.New("logic error")
	}

	// TODO:
	if op.Ctx.CacheVersion != op.Handle.GetVersion() {
		return errors.New(fmt.Sprintf("CacheVersion %d mistach expect %d", op.Ctx.CacheVersion, op.Handle.GetVersion()))
	}

	latest_ss := md.CacheHolder.GetSnapshot()
	next_seg_id, err := latest_ss.GetNextSegmentID()
	if err != nil {
		return err
	}
	if op.Ctx.Segment.ID.ID != next_seg_id {
		return errors.New(fmt.Sprintf("Abort CreateSegmentOperation due to race condition"))
	}
	ctx := md.CommitAddSegmentContext{
		Segment: op.Ctx.Segment,
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
