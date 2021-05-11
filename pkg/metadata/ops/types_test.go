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
	var segment_id uint64
	for k, _ := range ss.SegmentIDs() {
		segment_id = k
		break
	}
	opCtx := OperationContext{SegmentID: &segment_id, CacheVersion: ss.GetVersion()}
	op := NewCreateBlockOperation(&opCtx, ss, worker)
	blk, err := op.CommitNewBlock()
	assert.Nil(t, err)
	t.Log(blk.String())

	// err = op.OnExecute()
	op.Push()
	err = op.WaitDone()
	assert.Nil(t, err)

	new_ss := md.CacheHolder.GetSnapshot()
	assert.Equal(t, new_ss.GetVersion(), ss.GetVersion()+1)
	// t.Log(new_ss.String())

	segment, err := new_ss.GetSegment(segment_id)
	assert.Nil(t, err)
	t.Log(segment.String())
	// assert.Equal(t, segment.BlockIDs())
	// t.Log(new_ss.GetSegmentBlockIDs(segment_id))
	assert.Equal(t, len(new_ss.GetSegmentBlockIDs(segment_id)), 1)
	t.Log(new_ss.Cache.CheckPoint.String())
	worker.Stop()
}

func TestFlush(t *testing.T) {
	//1. Flush a block
	//2. Flush a metafile
	//3. Refresh the cache
}
