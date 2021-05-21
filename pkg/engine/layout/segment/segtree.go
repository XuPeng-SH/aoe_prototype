package segment

import (
	"aoe/pkg/common/util"
	"fmt"
)

var (
	_ ISegmentTree = (*SegmentTree)(nil)
)

func NewSegmentTree() ISegmentTree {
	tree := &SegmentTree{}
	return tree
}

func (tree *SegmentTree) WhichSeg(row uint64) ISegment {
	seg_idx := tree.WhichSegIdx(row)
	if seg_idx == util.UINT64_MAX {
		return nil
	}
	return tree.Segments[seg_idx]
}

func (tree *SegmentTree) WhichSegIdx(row uint64) uint64 {
	if len(tree.Segments) == 0 {
		return util.UINT64_MAX
	}
	lower := uint64(0)
	upper := uint64(len(tree.Segments) - 1)

	for lower <= upper {
		mid := (lower + upper) / 2
		if row < tree.Segments[mid].GetStartRow() {
			upper = mid - 1
		} else if row >= tree.Segments[mid].GetEndRow() {
			lower = mid + 1
		} else {
			return mid
		}
	}
	return util.UINT64_MAX
}

func (tree *SegmentTree) GetRoot() ISegment {
	if len(tree.Segments) == 0 {
		return nil
	}
	return tree.Segments[0]
}

func (tree *SegmentTree) GetTail() ISegment {
	if len(tree.Segments) == 0 {
		return nil
	}
	return tree.Segments[len(tree.Segments)-1]
	return nil
}

func (tree *SegmentTree) Depth() uint64 {
	return uint64(len(tree.Segments))
}

func (tree *SegmentTree) Append(new_seg ISegment) {
	tree.Segments = append(tree.Segments, new_seg)
}

func (tree *SegmentTree) String() string {
	depth := tree.Depth()
	if depth > 10 {
		depth = 10
	}
	return tree.ToString(depth)
}

func (tree *SegmentTree) ToString(depth uint64) string {
	if depth > tree.Depth() {
		depth = tree.Depth()
	}
	ret := fmt.Sprintf("SegTree (%v/%v) [", depth, tree.Depth())
	for i := uint64(0); i < depth; i++ {
		ret += tree.Segments[i].ToString(false)
		if i != depth-1 {
			ret += ","
		}
	}

	ret += "]"

	return ret
}
