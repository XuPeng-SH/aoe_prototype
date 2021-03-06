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
	// sync.Locker
	// RLock()
	// RUnlock()
	String() string
	ToString(depth uint64) string
	GetRoot() IColumnSegment
	GetTail() IColumnSegment
	Depth() uint64
	// ReferenceOther(other ISegmentTree)

	// Modifier
	Append(seg IColumnSegment) error
	UpgradeBlock(blkID layout.ID) IColumnBlock
	UpgradeSegment(segID layout.ID) IColumnSegment
	DropSegment(id layout.ID) (seg IColumnSegment, err error)
}

type SegmentTree struct {
	data struct {
		sync.RWMutex
		Segments []IColumnSegment
		Helper   map[layout.ID]int
	}
}

func NewSegmentTree() ISegmentTree {
	tree := &SegmentTree{}
	tree.data.Segments = make([]IColumnSegment, 0)
	tree.data.Helper = make(map[layout.ID]int)
	runtime.SetFinalizer(tree, func(o *SegmentTree) {
		log.Infof("[GC]: SegmentTree: %s", o.String())
		o.data.Segments = nil
	})
	return tree
}

func (tree *SegmentTree) DropSegment(id layout.ID) (seg IColumnSegment, err error) {
	idx, ok := tree.data.Helper[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Specified seg %s not found", id.SegmentString()))
	}
	seg = tree.data.Segments[idx]
	tree.data.Lock()
	defer tree.data.Unlock()
	if idx > 0 {
		prev := tree.data.Segments[idx-1]
		prev.SetNext(seg.GetNext())
	}
	delete(tree.data.Helper, id)
	tree.data.Segments = append(tree.data.Segments[:idx], tree.data.Segments[idx+1:]...)
	for idx, segment := range tree.data.Segments {
		tree.data.Helper[segment.GetID()] = idx
	}
	return seg, nil
}

func (tree *SegmentTree) Depth() uint64 {
	tree.data.RLock()
	defer tree.data.RUnlock()
	return uint64(len(tree.data.Segments))
}

func (tree *SegmentTree) GetRoot() IColumnSegment {
	tree.data.RLock()
	defer tree.data.RUnlock()
	if len(tree.data.Segments) == 0 {
		return nil
	}
	return tree.data.Segments[0]
}

func (tree *SegmentTree) GetTail() IColumnSegment {
	tree.data.RLock()
	defer tree.data.RUnlock()
	if len(tree.data.Segments) == 0 {
		return nil
	}
	return tree.data.Segments[len(tree.data.Segments)-1]
}

func (tree *SegmentTree) UpgradeBlock(blkID layout.ID) IColumnBlock {
	idx, ok := tree.data.Helper[blkID.AsSegmentID()]
	if !ok {
		panic("logic error")
	}
	seg := tree.data.Segments[idx]
	blk, err := seg.UpgradeBlock(blkID)
	if err != nil {
		panic(fmt.Sprintf("logic error: %s", err))
	}
	return blk
}

func (tree *SegmentTree) UpgradeSegment(segID layout.ID) IColumnSegment {
	idx, ok := tree.data.Helper[segID]
	if !ok {
		panic("logic error")
	}
	seg := tree.data.Segments[idx]

	if seg.GetSegmentType() != UNSORTED_SEG {
		panic("logic error")
	}
	if !segID.IsSameSegment(seg.GetID()) {
		panic("logic error")
	}

	upgradeSeg := seg.CloneWithUpgrade()
	if upgradeSeg == nil {
		panic(fmt.Sprintf("Cannot upgrade seg: %s", segID.SegmentString()))
	}
	var old_next IColumnSegment
	if idx != len(tree.data.Segments)-1 {
		old_next = seg.GetNext()
	}
	upgradeSeg.SetNext(old_next)
	tree.data.Lock()
	defer tree.data.Unlock()
	tree.data.Segments[idx] = upgradeSeg
	if idx > 0 {
		tree.data.Segments[idx-1].SetNext(upgradeSeg)
	}
	return upgradeSeg
}

func (tree *SegmentTree) Append(seg IColumnSegment) error {
	tree.data.Lock()
	defer tree.data.Unlock()
	_, ok := tree.data.Helper[seg.GetID()]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate seg %v in tree", seg.GetID()))
	}
	if len(tree.data.Segments) != 0 {
		tree.data.Segments[len(tree.data.Segments)-1].SetNext(seg)
	}
	tree.data.Segments = append(tree.data.Segments, seg)
	tree.data.Helper[seg.GetID()] = len(tree.data.Segments) - 1
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
		ret += tree.data.Segments[i].ToString(true)
		if i != depth-1 {
			ret += ","
		}
	}

	ret += "]"

	return ret
}
