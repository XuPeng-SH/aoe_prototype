package base

import ()

type IOpInternal interface {
	PreExecute() error
	Execute() error
	PostExecute() error
}

type IOp interface {
	OnExecute() error
	SetError(err error)
}
