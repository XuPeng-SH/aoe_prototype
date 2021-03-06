package coldata

import (
	"aoe/pkg/engine/layout"
	"aoe/pkg/engine/layout/table"
	"aoe/pkg/engine/layout/table/col"
	// log "github.com/sirupsen/logrus"
)

func NewUpgradeBlkOp(ctx *OpCtx, blkID layout.ID, td table.ITableData) *UpgradeBlkOp {
	op := &UpgradeBlkOp{
		BlockID:   blkID,
		TableData: td,
	}
	op.Op = *NewOp(op, ctx, ctx.Opts.MemData.Updater)
	return op
}

type UpgradeBlkOp struct {
	Op
	BlockID   layout.ID
	TableData table.ITableData
	Blocks    []col.IColumnBlock
}

func (op *UpgradeBlkOp) Execute() error {
	op.Blocks = op.TableData.UpgradeBlock(op.BlockID)
	return nil
}
