package ops

import (
	md "aoe/pkg/metadata3"
	// log "github.com/sirupsen/logrus"
)

func NewCreateTableOperation(ctx *OperationContext, info *md.MetaInfo,
	w IOpWorker) *CreateTableOperation {
	op := &CreateTableOperation{}
	op.Operation = *NewOperation(op, ctx, info, w)
	return op
}

type CreateTableOperation struct {
	Operation
}

func (op *CreateTableOperation) GetTable() *md.Table {
	tbl := op.Result.(*md.Table)
	return tbl
}

func (op *CreateTableOperation) execute() error {
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
