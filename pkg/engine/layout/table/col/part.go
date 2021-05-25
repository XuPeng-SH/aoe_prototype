package col

import (
	bmgrif "aoe/pkg/engine/buffer/manager/iface"
	nif "aoe/pkg/engine/buffer/node/iface"
	dio "aoe/pkg/engine/dataio"
	"aoe/pkg/engine/layout"
	ldio "aoe/pkg/engine/layout/dataio"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	log "github.com/sirupsen/logrus"
)

type ColumnPartAllocator struct {
}

func (alloc *ColumnPartAllocator) Malloc() (buf []byte, err error) {
	return buf, err
}

type IColumnPart interface {
	io.Closer
	GetNext() IColumnPart
	SetNext(IColumnPart)
	InitScanCursor(cursor *ScanCursor) error
	GetID() layout.ID
	GetBlock() IColumnBlock
	GetBuf() []byte
	GetColIdx() int
	CloneWithUpgrade(blk IColumnBlock) IColumnPart
}

type ColumnPart struct {
	sync.RWMutex
	ID          layout.ID
	Next        IColumnPart
	Block       IColumnBlock
	BufMgr      bmgrif.IBufferManager
	BufNode     nif.INodeHandle
	TypeSize    uint64
	MaxRowCount uint64
	RowCount    uint64
	Size        uint64
	Capacity    uint64
}

func NewColumnPart(bmgr bmgrif.IBufferManager, blk IColumnBlock, id layout.ID,
	rowCount uint64, typeSize uint64) IColumnPart {
	part := &ColumnPart{
		BufMgr:      bmgr,
		ID:          id,
		Block:       blk,
		TypeSize:    typeSize,
		MaxRowCount: rowCount,
	}

	switch blk.GetBlockType() {
	case TRANSIENT_BLK:
		part.BufNode = bmgr.RegisterSpillableNode(typeSize*rowCount, id)
	case PERSISTENT_BLK:
		sf := ldio.NewUnsortedSegmentFile(dio.READER_FACTORY.Dirname, id.AsSegmentID())
		csf := ldio.ColSegmentFile{
			SegmentFile: sf,
			ColIdx:      uint64(part.Block.GetColIdx()),
		}
		part.BufNode = bmgr.RegisterNode(typeSize*rowCount, id, &csf)
	case PERSISTENT_SORTED_BLK:
		sf := ldio.NewSortedSegmentFile(dio.READER_FACTORY.Dirname, id.AsSegmentID())
		csf := ldio.ColSegmentFile{
			SegmentFile: sf,
			ColIdx:      uint64(part.Block.GetColIdx()),
		}
		part.BufNode = bmgr.RegisterNode(typeSize*rowCount, id, &csf)
	case MOCK_BLK:
		csf := ldio.MockColSegmentFile{}
		part.BufNode = bmgr.RegisterNode(typeSize*rowCount, id, &csf)
	default:
		panic("not support")
	}
	runtime.SetFinalizer(part, func(p IColumnPart) {
		id := p.GetID()
		log.Infof("GC ColumnPart %s", id.String())
		p.SetNext(nil)
		p.Close()
	})

	blk.Append(part)
	return part
}

func (part *ColumnPart) CloneWithUpgrade(blk IColumnBlock) IColumnPart {
	cloned := &ColumnPart{
		ID:          part.ID,
		Block:       blk,
		BufMgr:      part.BufMgr,
		TypeSize:    part.TypeSize,
		MaxRowCount: part.MaxRowCount,
		RowCount:    part.RowCount,
		Size:        part.Size,
		Capacity:    part.Capacity,
	}
	switch part.Block.GetBlockType() {
	case TRANSIENT_BLK:
		sf := ldio.NewUnsortedSegmentFile(dio.READER_FACTORY.Dirname, part.ID.AsSegmentID())
		csf := ldio.ColSegmentFile{
			SegmentFile: sf,
			ColIdx:      uint64(part.Block.GetColIdx()),
		}
		cloned.BufNode = part.BufMgr.RegisterNode(part.MaxRowCount*part.TypeSize, part.ID, &csf)
	case PERSISTENT_BLK:
		sf := ldio.NewSortedSegmentFile(dio.READER_FACTORY.Dirname, part.ID.AsSegmentID())
		csf := ldio.ColSegmentFile{
			SegmentFile: sf,
			ColIdx:      uint64(part.Block.GetColIdx()),
		}
		cloned.BufNode = part.BufMgr.RegisterNode(part.MaxRowCount*part.TypeSize, part.ID, &csf)
	case PERSISTENT_SORTED_BLK:
		panic("logic error")
	default:
		panic("not supported")
	}

	// cloned.Next = part.Next
	runtime.SetFinalizer(cloned, func(p IColumnPart) {
		id := p.GetID()
		log.Infof("GC ColumnPart %s", id.String())
		p.SetNext(nil)
		p.Close()
	})
	return nil
}

func (part *ColumnPart) GetColIdx() int {
	return part.Block.GetColIdx()
}

func (part *ColumnPart) GetBuf() []byte {
	part.RLock()
	defer part.RUnlock()
	return part.BufNode.GetBuffer().GetDataNode().Data
}

func (part *ColumnPart) SetRowCount(cnt uint64) {
	if cnt > part.MaxRowCount {
		panic("logic error")
	}
	part.Lock()
	defer part.Unlock()
	part.RowCount = cnt
}

func (part *ColumnPart) SetSize(size uint64) {
	if size > part.Capacity {
		panic("logic error")
	}
	part.Lock()
	defer part.Unlock()
	part.Size = size
}

func (part *ColumnPart) GetID() layout.ID {
	return part.ID
}

func (part *ColumnPart) GetBlock() IColumnBlock {
	part.RLock()
	defer part.RUnlock()
	return part.Block
}

func (part *ColumnPart) SetNext(next IColumnPart) {
	part.Lock()
	defer part.Unlock()
	part.Next = next
}

func (part *ColumnPart) GetNext() IColumnPart {
	part.RLock()
	n := part.Next
	part.RUnlock()
	if n == nil {
		next_blk := part.Block.GetNext()
		if next_blk != nil {
			return next_blk.GetPartRoot()
		}
	}
	return n
}

func (part *ColumnPart) Close() error {
	if part.BufNode != nil {
		err := part.BufNode.Close()
		if err != nil {
			panic("logic error")
		}
		part.BufNode = nil
	}
	return nil
}

func (part *ColumnPart) InitScanCursor(cursor *ScanCursor) error {
	bufMgr := part.BufMgr
	cursor.Handle = bufMgr.Pin(part.BufNode)
	if cursor.Handle == nil {
		return errors.New(fmt.Sprintf("Cannot pin part %v", part.ID))
	}
	return nil
}
