package coldata

import (
	"aoe/pkg/engine/layout"
	"aoe/pkg/engine/layout/table"
	"aoe/pkg/engine/layout/table/col"
	// log "github.com/sirupsen/logrus"
)

func NewUpgradeSegOp(ctx *OpCtx, segID layout.ID, td table.ITableData) *UpgradeSegOp {
	op := &UpgradeSegOp{
		SegmentID: segID,
		TableData: td,
	}
	op.Op = *NewOp(op, ctx, ctx.Opts.MemData.Updater)
	return op
}

type UpgradeSegOp struct {
	Op
	SegmentID layout.ID
	TableData table.ITableData
	Segments  []col.IColumnSegment
}

func (op *UpgradeSegOp) Execute() error {
	op.Segments = op.TableData.UpgradeSegment(op.SegmentID)
	return nil
}
