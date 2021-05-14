package meta

import (
	md "aoe/pkg/engine/metadata"
	"aoe/pkg/engine/ops"
	iworker "aoe/pkg/engine/worker/base"
	// log "github.com/sirupsen/logrus"
)

func NewCreateTableOperation(ctx *ops.OperationContext, info *md.MetaInfo,
	w iworker.IOpWorker) *CreateTableOperation {
	op := &CreateTableOperation{}
	op.Operation = *ops.NewOperation(op, ctx, info, w)
	return op
}

type CreateTableOperation struct {
	ops.Operation
}

func (op *CreateTableOperation) GetTable() *md.Table {
	tbl := op.Result.(*md.Table)
	return tbl
}

func (op *CreateTableOperation) Execute() error {
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
