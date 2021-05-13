package md2

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestBlock(t *testing.T) {
	ts1 := NowMicro()
	time.Sleep(time.Duration(1) * time.Microsecond)
	blk := NewBlock(Meta.Sequence.GetTableID(), Meta.Sequence.GetSegmentID(), Meta.Sequence.GetBlockID())
	time.Sleep(time.Duration(1) * time.Microsecond)
	ts2 := NowMicro()
	t.Logf("%d %d %d", ts1, blk.CreatedOn, ts2)
	assert.False(t, blk.Select(ts1))
	assert.True(t, blk.Select(ts2))
	time.Sleep(time.Duration(1) * time.Microsecond)
	ts3 := NowMicro()

	err := blk.Deltete(ts3)
	assert.Nil(t, err)
	time.Sleep(time.Duration(1) * time.Microsecond)
	ts4 := NowMicro()

	assert.False(t, blk.Select(ts1))
	assert.True(t, blk.Select(ts2))
	assert.False(t, blk.Select(ts3))
	assert.False(t, blk.Select(ts4))
}

func TestSegment(t *testing.T) {
	t1 := NowMicro()
	seg1 := NewSegment(Meta.Sequence.GetTableID(), Meta.Sequence.GetSegmentID())
	blk1 := NewBlock(seg1.GetTableID(), Meta.Sequence.GetSegmentID(), Meta.Sequence.GetBlockID())
	err := seg1.RegisterBlock(blk1)
	assert.Error(t, err)

	for i := 0; i < int(seg1.MaxBlockCount); i++ {
		blk1, err = seg1.CreateBlock()
		assert.Nil(t, err)
		err = seg1.RegisterBlock(blk1)
		assert.Nil(t, err)
	}
	blk2 := NewBlock(seg1.GetTableID(), seg1.GetID(), Meta.Sequence.GetBlockID())
	err = seg1.RegisterBlock(blk2)
	assert.Error(t, err)
	t.Log(err)

	_, err = seg1.ReferenceBlock(blk1.ID)
	assert.Nil(t, err)
	_, err = seg1.ReferenceBlock(blk2.ID)
	assert.Error(t, err)
	t.Log(seg1.String())

	ids := seg1.BlockIDs(t1)
	assert.Equal(t, len(ids), 0)
	// ts := NowMicro()
	ids = seg1.BlockIDs()
	assert.Equal(t, len(ids), int(seg1.MaxBlockCount))
}

func TestTable(t *testing.T) {
	bkt := NewTable()
	seg, err := bkt.CreateSegment()
	assert.Nil(t, err)

	assert.Equal(t, seg.GetBoundState(), STANDLONE)

	err = bkt.RegisterSegment(seg)
	assert.Nil(t, err)
	t.Log(bkt.String())
	assert.Equal(t, seg.GetBoundState(), Attached)
}
