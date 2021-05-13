package ops

import (
	md "aoe/pkg/metadata3"
	// "errors"
	// log "github.com/sirupsen/logrus"
)

type OperationContext struct {
	TableID     uint64
	Block       *md.Block
	Segment     *md.Segment
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
	ResultC  chan error
	Worker   IOpWorker
}
