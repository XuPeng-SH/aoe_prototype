package meta

import (
	md "aoe/pkg/engine/metadata"
	"aoe/pkg/engine/ops"
	iworker "aoe/pkg/engine/worker/base"
	// log "github.com/sirupsen/logrus"
)

func NewCreateTblOp(ctx *ops.OpCtx, info *md.MetaInfo,
	w iworker.IOpWorker) *CreateTblOp {
	op := &CreateTblOp{}
	op.Op = *ops.NewOp(op, ctx, info, w)
	return op
}

type CreateTblOp struct {
	ops.Op
}

func (op *CreateTblOp) GetTable() *md.Table {
	tbl := op.Result.(*md.Table)
	return tbl
}

func (op *CreateTblOp) Execute() error {
	tbl, err := op.MetaInfo.CreateTable()
	if err != nil {
		return err
	}

	err = op.MetaInfo.RegisterTable(tbl)
	if err == nil {
		op.Result = tbl
	}
	return err
}
