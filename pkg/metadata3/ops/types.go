package ops

import (
	md "aoe/pkg/metadata3"
	// "errors"
	// log "github.com/sirupsen/logrus"
)

type OperationContext struct {
	TableID     uint64
	TmpMetaFile string
}

type IOperationInternal interface {
	preExecute() error
	execute() error
	postExecute() error
}

type IOperation interface {
	OnExecute() error
	SetError(err error)
}

type Operation struct {
	Ctx      *OperationContext
	MetaInfo *md.MetaInfo
	Impl     IOperationInternal
	ErrorC   chan error
	Worker   IOpWorker
	Err      error
	Result   interface{}
}
