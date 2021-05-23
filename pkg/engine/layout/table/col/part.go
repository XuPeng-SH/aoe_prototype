package col

import (
	bmgrif "aoe/pkg/engine/buffer/manager/iface"
	nif "aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	"errors"
	"fmt"
	"io"
	// log "github.com/sirupsen/logrus"
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
}

type ColumnPart struct {
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

	if blk.GetBlockType() == TRANSIENT_BLK {
		part.BufNode = bmgr.RegisterSpillableNode(typeSize*rowCount, id)
	} else {
		part.BufNode = bmgr.RegisterNode(typeSize*rowCount, id)
	}

	blk.Append(part)
	return part
}

func (part *ColumnPart) GetBuf() []byte {
	return part.BufNode.GetBuffer().GetDataNode().Data
}

func (part *ColumnPart) SetRowCount(cnt uint64) {
	if cnt > part.MaxRowCount {
		panic("logic error")
	}
	part.RowCount = cnt
}

func (part *ColumnPart) SetSize(size uint64) {
	if size > part.Capacity {
		panic("logic error")
	}
	part.Size = size
}

func (part *ColumnPart) GetID() layout.ID {
	return part.ID
}

func (part *ColumnPart) GetBlock() IColumnBlock {
	return part.Block
}

func (part *ColumnPart) SetNext(next IColumnPart) {
	part.Next = next
}

func (part *ColumnPart) GetNext() IColumnPart {
	n := part.Next
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
	cursor.Handle = part.BufMgr.Pin(part.BufNode)
	if cursor.Handle == nil {
		return errors.New(fmt.Sprintf("Cannot pin part %v", part.ID))
	}
	return nil
}
