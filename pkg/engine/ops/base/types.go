package base

import ()

type IOperationInternal interface {
	PreExecute() error
	Execute() error
	PostExecute() error
}

type IOperation interface {
	OnExecute() error
	SetError(err error)
}
