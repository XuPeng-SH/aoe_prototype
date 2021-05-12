package md2

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestBlock(t *testing.T) {
	ts1 := NowMicro()
	time.Sleep(time.Duration(1) * time.Microsecond)
	blk := NewBlock(SEQUENCE.GetBucketID(), SEQUENCE.GetSegmentID(), SEQUENCE.GetBlockID())
	time.Sleep(time.Duration(1) * time.Microsecond)
	ts2 := NowMicro()
	t.Logf("%d %d %d", ts1, blk.CreatedOn, ts2)
	assert.False(t, blk.Select(ts1))
	assert.True(t, blk.Select(ts2))
}
