package meta

import (
	"aoe/pkg/engine/layout"
	"aoe/pkg/engine/layout/table"
	"aoe/pkg/engine/layout/table/col"
	md "aoe/pkg/engine/metadata"
	// log "github.com/sirupsen/logrus"
)

func NewCreateBlkOp(ctx *OpCtx, tid uint64, tableData table.ITableData) *CreateBlkOp {
	op := &CreateBlkOp{
		TableData: tableData,
		ColBlocks: make([]col.IColumnBlock, 0),
		TableID:   tid,
	}
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Updater)
	return op
}

type CreateBlkOp struct {
	Op
	NewSegment bool
	TableData  table.ITableData
	ColBlocks  []col.IColumnBlock
	TableID    uint64
}

func (op *CreateBlkOp) HasNewSegment() bool {
	return op.NewSegment
}

func (op *CreateBlkOp) GetBlock() *md.Block {
	if op.Err != nil {
		return nil
	}
	return op.Result.(*md.Block)
}

func (op *CreateBlkOp) Execute() error {
	table, err := op.Ctx.Opts.Meta.Info.ReferenceTable(op.TableID)
	if err != nil {
		return err
	}

	seg, err := table.GetInfullSegment()
	if err != nil {
		seg, err = table.CreateSegment()
		if err != nil {
			return err
		}
		err = table.RegisterSegment(seg)
		if err != nil {
			return err
		}
		op.NewSegment = true
	}
	blk, err := seg.CreateBlock()
	if err != nil {
		return err
	}
	err = seg.RegisterBlock(blk)
	if err != nil {
		return err
	}
	cloned, err := seg.CloneBlock(blk.ID)
	if err != nil {
		return err
	}
	op.Result = cloned
	if op.TableData != nil {
		op.registerTableData(blk)
	}
	return err
}

func (op *CreateBlkOp) registerTableData(blk *md.Block) {
	blk_id := layout.ID{
		TableID:   blk.TableID,
		SegmentID: blk.SegmentID,
		BlockID:   blk.ID,
	}
	for _, column := range op.TableData.GetCollumns() {
		if op.NewSegment {
			_, err := column.RegisterSegment(blk_id.AsBlockID())
			if err != nil {
				panic("should not happend")
			}
		}
		colBlk, _ := column.RegisterBlock(op.TableData.GetBufMgr(), blk_id, blk.MaxRowCount)
		op.ColBlocks = append(op.ColBlocks, colBlk)
	}
}
