package col

import (
	bmgrif "aoe/pkg/engine/buffer/manager/iface"
	nif "aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	"errors"
	"fmt"
)

type StdColumnBlock struct {
	ColumnBlock
	BufMgr  bmgrif.IBufferManager
	BufNode nif.INodeHandle
}

func NewStdColumnBlock(bmgr bmgrif.IBufferManager, seg IColumnSegment, id layout.BlockId,
	rowCount uint64, typeSize uint64) IColumnBlock {
	blk := &StdColumnBlock{
		BufMgr: bmgr,
		ColumnBlock: ColumnBlock{
			ID:       id,
			Segment:  seg,
			RowCount: rowCount,
		},
	}
	blk.BufNode = bmgr.RegisterNode(rowCount*typeSize, id)
	seg.Append(blk)
	return blk
}

func (blk *StdColumnBlock) Close() error {
	if blk.BufNode != nil {
		err := blk.BufNode.Close()
		if err != nil {
			panic("logic error")
		}
		blk.BufNode = nil
	}
	return nil
}

func (blk *StdColumnBlock) InitScanCursor(cursor *ScanCursor) error {
	cursor.Handle = blk.BufMgr.Pin(blk.BufNode)
	if cursor.Handle == nil {
		return errors.New(fmt.Sprintf("Cannot pin blk %v", blk.ID))
	}
	return nil
}
