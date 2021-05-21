package segment

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func add_new_segment(t *testing.T, seg ISegment) {
	new_seg := NewSegment(1024, 1024)
	seg.Append(new_seg)
}

func TestSegment(t *testing.T) {
	start := uint64(0)
	count := uint64(1024)
	seg := NewSegment(start, count)
	assert.Equal(t, seg.GetStartRow(), start)
	assert.Equal(t, seg.GetRowCount(), count)
	assert.True(t, seg.GetNext() == nil)
	add_new_segment(t, seg)
	assert.True(t, seg.GetNext() != nil)
	t.Log(seg.String())
}
