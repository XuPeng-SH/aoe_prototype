package ops

import (
	md "aoe/pkg/metadata"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateSegmentOp(t *testing.T) {
	ss := md.CacheHolder.GetSnapshot()
	t.Log(ss.String())

	opCtx := OperationContext{CacheVersion: ss.GetVersion()}
	op := NewCreateSegmentOperation(&opCtx, ss)
	seg, err := op.CommitNewSegment()
	assert.Nil(t, err)
	assert.Equal(t, ss.GetVersion(), uint64(0))
	t.Log(seg.String())
	t.Log(ss.String())

	op.OnExecute()
	t.Log(ss.String())
	assert.Equal(t, ss.GetVersion(), uint64(0))
	new_ss := md.CacheHolder.GetSnapshot()
	assert.Equal(t, new_ss.GetVersion(), uint64(1))
	t.Log(new_ss.String())
}

func TestCreateBlockOp(t *testing.T) {
	ss := md.CacheHolder.GetSnapshot()
	t.Log(ss.String())
	t.Log(ss.SegmentIDs())
	var segment_id uint64
	for k, _ := range ss.SegmentIDs() {
		segment_id = k
		break
	}
	opCtx := OperationContext{SegmentID: &segment_id, CacheVersion: ss.GetVersion()}
	op := NewCreateBlockOperation(&opCtx, ss)
	blk, err := op.CommitNewBlock()
	assert.Nil(t, err)
	t.Log(blk.String())

	err = op.OnExecute()
	assert.Nil(t, err)

	new_ss := md.CacheHolder.GetSnapshot()
	assert.Equal(t, new_ss.GetVersion(), ss.GetVersion()+1)
	t.Log(new_ss.String())
}
