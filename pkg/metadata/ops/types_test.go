package ops

import (
	md "aoe/pkg/metadata"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateSegmentOp(t *testing.T) {
	worker := NewOperationWorker()
	worker.Start()
	ss := md.CacheHolder.GetSnapshot()
	// t.Log(ss.String())

	opCtx := OperationContext{CacheVersion: ss.GetVersion()}
	op := NewCreateSegmentOperation(&opCtx, ss, worker)
	seg, err := op.CommitNewSegment()
	assert.Nil(t, err)
	assert.Equal(t, ss.GetVersion(), uint64(0))
	t.Log(seg.String())
	// t.Log(ss.String())

	op.Push()
	err = op.WaitDone()
	assert.Nil(t, err)
	// op.OnExecute()
	// t.Log(ss.String())
	assert.Equal(t, ss.GetVersion(), uint64(0))
	new_ss := md.CacheHolder.GetSnapshot()
	assert.Equal(t, new_ss.GetVersion(), uint64(1))
	// t.Log(new_ss.String())
	worker.Stop()
}

func TestCreateBlockOp(t *testing.T) {
	worker := NewOperationWorker()
	worker.Start()
	ss := md.CacheHolder.GetSnapshot()
	// var segment_id uint64
	// for k, _ := range ss.SegmentIDs() {
	// 	segment_id = k
	// 	break
	// }
	opCtx := OperationContext{CacheVersion: ss.GetVersion()}
	op := NewCreateBlockOperation(&opCtx, ss, worker)
	blk, err := op.CommitNewBlock()
	assert.Nil(t, err)
	t.Log(blk.String())
	assert.True(t, blk.IsActive())

	op.Push()
	err = op.WaitDone()
	assert.Nil(t, err)

	new_ss := md.CacheHolder.GetSnapshot()
	assert.Equal(t, new_ss.GetVersion(), ss.GetVersion()+1)
	segment, err := new_ss.GetSegment(blk.SegmentID)
	assert.Nil(t, err)
	t.Log(segment.String())
	assert.Equal(t, len(new_ss.GetSegmentBlockIDs(blk.SegmentID)), 1)

	opCtx = OperationContext{CacheVersion: new_ss.GetVersion()}
	blk2, err := new_ss.GetBlock(blk.SegmentID, blk.ID.ID)
	blk3 := blk2.Copy()
	assert.Equal(t, blk3.DataState, md.EMPTY)
	assert.True(t, blk3.IsActive())
	blk3.SetCount(blk3.MaxRowCount / 2)
	assert.Equal(t, blk3.DataState, md.PARTIAL)
	assert.True(t, blk3.IsActive())
	blk3.SetCount(blk3.MaxRowCount)
	assert.Equal(t, blk3.DataState, md.FULL)
	assert.False(t, blk3.IsActive())
	opCtx.Block = blk3
	updateblkop := NewUpdateBlockOperation(&opCtx, new_ss, worker)
	updateblkop.Push()
	err = updateblkop.WaitDone()
	assert.Nil(t, err)
	blk4, err := new_ss.GetBlock(blk3.SegmentID, blk3.ID.ID)
	assert.Nil(t, err)
	assert.True(t, blk4.IsActive())
	seg, err := new_ss.GetSegment(blk3.SegmentID)
	assert.Nil(t, err)
	assert.True(t, seg.IsActive())
	assert.Equal(t, seg.DataState, md.EMPTY)
	new_ss = md.CacheHolder.GetSnapshot()
	// t.Log(new_ss.String())
	blk5, err := new_ss.GetBlock(blk3.SegmentID, blk3.ID.ID)
	assert.Nil(t, err)
	assert.False(t, blk5.IsActive())

	seg, err = new_ss.GetSegment(blk3.SegmentID)
	assert.Nil(t, err)
	assert.True(t, seg.IsActive())
	assert.Equal(t, seg.DataState, md.PARTIAL)
	seg2 := new_ss.Cache.Delta.GetActiveSegment()
	assert.Equal(t, seg.ID.ID, seg2.ID.ID)
	blk6, err := seg2.GetActiveBlock()
	assert.Nil(t, blk6)
	assert.Nil(t, err)

	opCtx = OperationContext{CacheVersion: new_ss.GetVersion()}
	opCtx.Block = blk
	flushop := NewFlushOperation(&opCtx, new_ss, worker)
	flushop.Push()
	err = flushop.WaitDone()
	assert.Nil(t, err)

	// latest_ss := md.CacheHolder.GetSnapshot()
	// t.Log(ss.String())
	// t.Log(new_ss.String())
	// t.Log(latest_ss.String())

	worker.Stop()
}

func TestFlush(t *testing.T) {
	// worker := NewOperationWorker()
	// worker.Start()

	// ss := md.CacheHolder.GetSnapshot()
	// ss.GetSe

	// worker.Stop()
}
