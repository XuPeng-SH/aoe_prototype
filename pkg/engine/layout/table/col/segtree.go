package col

import (
	"aoe/pkg/engine/layout"
	"errors"
	"fmt"
	"sync"
)

type ISegmentTree interface {
	String() string
	ToString(depth uint64) string
	GetRoot() IColumnSegment
	GetTail() IColumnSegment
	Depth() uint64
	Append(seg IColumnSegment) error
	// ReferenceOther(other ISegmentTree)
}

type SegmentTree struct {
	sync.RWMutex
	Segments []IColumnSegment
	Helper   map[layout.BlockId]bool
}

func (tree *SegmentTree) Depth() uint64 {
	return uint64(len(tree.Segments))
}

func (tree *SegmentTree) GetRoot() IColumnSegment {
	if len(tree.Segments) == 0 {
		return nil
	}
	return tree.Segments[0]
}

func (tree *SegmentTree) GetTail() IColumnSegment {
	if len(tree.Segments) == 0 {
		return nil
	}
	return tree.Segments[len(tree.Segments)-1]
}

func (tree *SegmentTree) Append(seg IColumnSegment) error {
	_, ok := tree.Helper[seg.GetID()]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate seg %v in tree", seg.GetID()))
	}
	tree.Segments = append(tree.Segments, seg)
	tree.Helper[seg.GetID()] = true
	return nil
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
