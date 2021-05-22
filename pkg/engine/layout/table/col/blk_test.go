package col

import (
	bmgr "aoe/pkg/engine/buffer/manager"
	dio "aoe/pkg/engine/dataio"
	"aoe/pkg/engine/layout"
	w "aoe/pkg/engine/worker"
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

var WORK_DIR = "/tmp/layout/blk_test"

func init() {
	dio.WRITER_FACTORY.Init(nil, WORK_DIR)
	dio.READER_FACTORY.Init(nil, WORK_DIR)
}

func TestStdColumnBlock(t *testing.T) {
	baseid := layout.BlockId{}
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	seg_cnt := 5
	for i := 0; i < seg_cnt; i++ {
		seg_id := baseid.NextSegment()
		seg := NewSegment(seg_id)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		blk_0_id := seg_id.NextBlock()
		blk_0 := NewStdColumnBlock(seg, blk_0_id)
		assert.Nil(t, blk_0.GetNext())
		assert.Equal(t, seg, blk_0.GetSegment())
		assert.Equal(t, blk_0, seg.GetBlockRoot())
		blk_1_id := seg_id.NextBlock()
		blk_1 := NewStdColumnBlock(seg, blk_1_id)
		assert.Nil(t, blk_1.GetNext())
		assert.Equal(t, blk_1, blk_0.GetNext())
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

func TestStdColumnBlock2(t *testing.T) {
	typeSize := uint64(unsafe.Sizeof(uint64(0)))
	row_count := uint64(64)
	capacity := typeSize * row_count
	flusher := w.NewOpWorker()
	bufMgr := bmgr.NewBufferManager(capacity, flusher)
	t.Log(bufMgr.GetCapacity())
	baseid := layout.BlockId{}
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	seg_cnt := 5
	for i := 0; i < seg_cnt; i++ {
		seg_id := baseid.NextSegment()
		seg := NewSegment(seg_id)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		blk_0_id := seg_id.NextBlock()
		blk_0 := NewStdColumnBlock(seg, blk_0_id)
		part_0 := NewColumnPart(bufMgr, blk_0, blk_0_id, row_count, typeSize)
		assert.Nil(t, part_0.GetNext())
		assert.Equal(t, blk_0, part_0.GetBlock())
		assert.Equal(t, part_0, blk_0.GetPartRoot())
		blk_1_id := seg_id.NextBlock()
		blk_1 := NewStdColumnBlock(seg, blk_1_id)
		part_1 := NewColumnPart(bufMgr, blk_1, blk_1_id, row_count, typeSize)
		assert.Nil(t, part_1.GetNext())
		assert.Equal(t, blk_1, part_1.GetBlock())
		assert.Equal(t, part_1, blk_1.GetPartRoot())
		// assert.Equal(t, row_count*2, seg.GetRowCount())
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

	first_part := first_seg.GetPartRoot()
	assert.NotNil(t, first_part)
	cursor := ScanCursor{
		Current: first_part,
	}

	for {
		err := cursor.Init()
		assert.Nil(t, err)
		if !cursor.Next() {
			break
		}
	}

	cursor.Close()
}

func TestStrColumnBlock(t *testing.T) {
	typeSize := uint64(unsafe.Sizeof(uint64(0)))
	row_count := uint64(1)
	capacity := typeSize * row_count
	flusher := w.NewOpWorker()
	bufMgr := bmgr.NewBufferManager(capacity, flusher)
	t.Log(bufMgr.GetCapacity())
	baseid := layout.BlockId{}
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	seg_cnt := 5
	for i := 0; i < seg_cnt; i++ {
		seg_id := baseid.NextSegment()
		seg := NewSegment(seg_id)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		blk_0_id := seg_id.NextBlock()
		blk_0 := NewStrColumnBlock(seg, blk_0_id)
		part_0_0_id := blk_0_id.NextPart()
		part_0 := NewColumnPart(bufMgr, blk_0, part_0_0_id, row_count, typeSize)
		assert.Nil(t, part_0.GetNext())
		assert.Equal(t, blk_0, part_0.GetBlock())
		assert.Equal(t, part_0, blk_0.GetPartRoot())
		part_0_1_id := blk_0_id.NextPart()
		part_0_1 := NewColumnPart(bufMgr, blk_0, part_0_1_id, row_count, typeSize)
		assert.Nil(t, part_0_1.GetNext())
		assert.Equal(t, blk_0, part_0_1.GetBlock())
		// assert.Equal(t, part_0, blk_0.GetPartRoot())
		if prev_seg != nil {
			prev_seg.SetNext(seg)
		} else {
			first_seg = seg
		}
		prev_seg = seg
	}
	t.Log(first_seg.ToString(true))
	part := first_seg.GetPartRoot()
	assert.NotNil(t, part)
	var cnt int
	for part != nil {
		t.Log(part.GetID())
		part = part.GetNext()
		cnt++
	}
	assert.Equal(t, seg_cnt*2, cnt)

	first_part := first_seg.GetPartRoot()
	assert.NotNil(t, first_part)
	cursor := ScanCursor{
		Current: first_part,
	}
	for {
		err := cursor.Init()
		assert.Nil(t, err)
		if !cursor.Next() {
			break
		}
	}

	cursor.Close()
}

type MockType struct {
}

func (t *MockType) Size() uint64 {
	return uint64(4)
}

func TestStdSegmentTree(t *testing.T) {
	baseid := layout.BlockId{}
	col_idx := uint64(0)
	col_data := NewColumnData(MockType{}, col_idx)

	seg_cnt := 5
	for i := 0; i < seg_cnt; i++ {
		seg_id := baseid.NextSegment()
		seg := NewSegment(seg_id)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		err := col_data.Append(seg)
		assert.Nil(t, err)
	}
	assert.Equal(t, uint64(seg_cnt), col_data.SegmentCount())
	t.Log(col_data.String())
	seg := col_data.GetSegmentRoot()
	assert.NotNil(t, seg)
	cnt := 0
	for {
		cnt++
		seg = seg.GetNext()
		if seg == nil {
			break
		}
	}
	assert.Equal(t, seg_cnt, cnt)
}
