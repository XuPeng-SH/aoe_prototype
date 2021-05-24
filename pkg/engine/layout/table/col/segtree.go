package col

import (
	"aoe/pkg/engine/layout"
	"errors"
	"fmt"
	"runtime"

	log "github.com/sirupsen/logrus"
	"sync"
)

type ISegmentTree interface {
	sync.Locker
	RLock()
	RUnlock()
	String() string
	ToString(depth uint64) string
	GetRoot() IColumnSegment
	GetTail() IColumnSegment
	Depth() uint64
	Append(seg IColumnSegment) error
	// ReferenceOther(other ISegmentTree)
	DropSegment(id layout.ID) (seg IColumnSegment, err error)
}

type SegmentTree struct {
	sync.RWMutex
	Segments []IColumnSegment
	Helper   map[layout.ID]int
}

func NewSegmentTree() ISegmentTree {
	tree := &SegmentTree{
		Segments: make([]IColumnSegment, 0),
		Helper:   make(map[layout.ID]int),
	}
	runtime.SetFinalizer(tree, func(o ISegmentTree) {
		log.Infof("[GC]: SegmentTree")
	})
	return tree
}

func (tree *SegmentTree) DropSegment(id layout.ID) (seg IColumnSegment, err error) {
	idx, ok := tree.Helper[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Specified seg %s not found", id.SegmentString()))
	}
	seg = tree.Segments[idx]
	if idx == 0 {
	} else {
		prev := tree.Segments[idx-1]
		prev.SetNext(seg.GetNext())
	}
	tree.Segments = append(tree.Segments[:idx], tree.Segments[idx+1:]...)
	return seg, nil
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
	if len(tree.Segments) != 0 {
		tree.Segments[len(tree.Segments)-1].SetNext(seg)
	}
	tree.Segments = append(tree.Segments, seg)
	tree.Helper[seg.GetID()] = len(tree.Segments) - 1
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
