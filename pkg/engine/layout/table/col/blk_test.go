package col

import (
	bmgr "aoe/pkg/engine/buffer/manager"
	dio "aoe/pkg/engine/dataio"
	"aoe/pkg/engine/layout"
	w "aoe/pkg/engine/worker"
	"github.com/stretchr/testify/assert"
	"testing"
	// "aoe/pkg/engine"
	// md "aoe/pkg/engine/metadata"
	// mops "aoe/pkg/engine/ops/meta"
	// todo "aoe/pkg/mock"
)

var WORK_DIR = "/tmp/layout/blk_test"

func init() {
	dio.WRITER_FACTORY.Init(nil, WORK_DIR)
	dio.READER_FACTORY.Init(nil, WORK_DIR)
}

func TestManager(t *testing.T) {
	// opts := &engine.Options{}
	capacity := uint64(4096)
	flusher := w.NewOpWorker()
	bufMgr := bmgr.NewBufferManager(capacity, flusher)
	baseid := layout.BlockId{}
	row_count := uint64(64)
	// seg0_id := baseid.NextSegment()
	// seg0 := NewSegment(seg0_id)
	// assert.Nil(t, seg0.GetNext())
	// assert.Nil(t, seg0.GetBlockRoot())
	// blk0_0_id := seg0_id.NextBlock()
	// blk0_0 := NewStdColumnBlock(bufMgr, seg0, blk0_0_id, row_count)
	// assert.Nil(t, blk0_0.GetNext())
	// assert.Equal(t, seg0, blk0_0.GetSegment())
	// assert.Equal(t, blk0_0, seg0.GetBlockRoot())
	// blk0_1_id := seg0_id.NextBlock()
	// blk0_1 := NewStdColumnBlock(bufMgr, seg0, blk0_1_id, row_count)
	// assert.Nil(t, blk0_1.GetNext())
	// assert.Equal(t, blk0_1, blk0_0.GetNext())
	// assert.Equal(t, seg0, blk0_1.GetSegment())
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	seg_cnt := 5
	for i := 0; i < seg_cnt; i++ {
		seg_id := baseid.NextSegment()
		seg := NewSegment(seg_id)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		blk_0_id := seg_id.NextBlock()
		blk_0 := NewStdColumnBlock(bufMgr, seg, blk_0_id, row_count)
		assert.Nil(t, blk_0.GetNext())
		assert.Equal(t, seg, blk_0.GetSegment())
		assert.Equal(t, blk_0, seg.GetBlockRoot())
		blk_1_id := seg_id.NextBlock()
		blk_1 := NewStdColumnBlock(bufMgr, seg, blk_1_id, row_count)
		assert.Nil(t, blk_1.GetNext())
		assert.Equal(t, blk_1, blk_0.GetNext())
		assert.Equal(t, row_count*2, seg.GetRowCount())
		if prev_seg != nil {
			prev_seg.SetNext(seg)
		} else {
			first_seg = seg
		}
		prev_seg = seg
	}
	t.Log(first_seg.ToString(true))
	blk := first_seg.GetBlockRoot()
	assert.NotNil(t, blk)
	var cnt int
	for blk != nil {
		t.Log(blk.GetID())
		blk = blk.GetNext()
		cnt++
	}
	assert.Equal(t, seg_cnt*2, cnt)
}
