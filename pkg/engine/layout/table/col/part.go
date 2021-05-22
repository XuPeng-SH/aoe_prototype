package col

import (
	bmgrif "aoe/pkg/engine/buffer/manager/iface"
	nif "aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	"errors"
	"fmt"
	"io"
)

type IColumnPart interface {
	io.Closer
	GetNext() IColumnPart
	InitScanCursor(cursor *ScanCursor) error
	GetID() layout.BlockId
	GetBlock() IColumnBlock
}

type ColumnPart struct {
	ID          layout.BlockId
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

func NewColumnPart(bmgr bmgrif.IBufferManager, blk IColumnBlock, id layout.BlockId,
	rowCount uint64, typeSize uint64) IColumnPart {
	part := &ColumnPart{
		BufMgr:      bmgr,
		ID:          id,
		Block:       blk,
		TypeSize:    typeSize,
		MaxRowCount: rowCount,
	}
	part.BufNode = bmgr.RegisterNode(typeSize*rowCount, id)
	blk.Append(part)
	return part
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

func (part *ColumnPart) GetID() layout.BlockId {
	return part.ID
}

func (part *ColumnPart) GetBlock() IColumnBlock {
	return part.Block
}

func (part *ColumnPart) GetNext() IColumnPart {
	next_blk := part.Block.GetNext()
	if next_blk != nil {
		return next_blk.GetPartRoot()
	}
	return nil
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
