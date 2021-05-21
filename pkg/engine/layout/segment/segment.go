package segment

import (
	"fmt"
)

var (
	_ ISegment = (*Segment)(nil)
)

func NewSegment(start, count uint64) ISegment {
	seg := &Segment{
		StartRow: start,
		RowCount: count,
	}
	return seg
}

func (seg *Segment) GetNext() ISegment {
	return seg.Next
}

func (seg *Segment) GetStartRow() uint64 {
	return seg.StartRow
}

func (seg *Segment) GetEndRow() uint64 {
	return seg.StartRow + seg.RowCount
}

func (seg *Segment) GetRowCount() uint64 {
	return seg.RowCount
}

func (seg *Segment) Capacity() uint64 {
	// PXU TODO
	return seg.RowCount
}

func (seg *Segment) Append(next ISegment) {
	seg.Next = next
}

func (seg *Segment) String() string {
	return seg.ToString(true)
}

func (seg *Segment) ToString(verbose bool) string {
	if verbose {
		return fmt.Sprintf("Seg(%v, %v)[HasNext:%v]", seg.StartRow, seg.RowCount, seg.Next != nil)
	}
	return fmt.Sprintf("(%v, %v)", seg.StartRow, seg.RowCount)
}
