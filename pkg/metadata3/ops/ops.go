package ops

import (
	md "aoe/pkg/metadata3"
	"errors"
	// log "github.com/sirupsen/logrus"
)

func NewOperation(impl IOperationInternal, ctx *OperationContext,
	info *md.MetaInfo, w IOpWorker) *Operation {
	op := &Operation{
		Ctx:      ctx,
		MetaInfo: info,
		Impl:     impl,
		ErrorC:   make(chan error),
		Worker:   w,
	}
	return op
}

func (op *Operation) Push() {
	op.Worker.SendOp(op)
}

func (op *Operation) SetError(err error) {
	op.Err = err
	op.ErrorC <- err
}

func (op *Operation) WaitDone() error {
	err := <-op.ErrorC
	return err
}

func (op *Operation) preExecute() error {
	if op.Ctx == nil {
		return errors.New("No context specified")
	}
	return nil
}

func (op *Operation) postExecute() error {
	return nil
}

func (op *Operation) execute() error {
	return nil
}

func (op *Operation) OnExecute() error {
	err := op.preExecute()
	if err != nil {
		return err
	}
	err = op.Impl.preExecute()
	if err != nil {
		return err
	}
	err = op.Impl.execute()
	if err != nil {
		return err
	}
	err = op.postExecute()
	if err != nil {
		return err
	}
	err = op.Impl.postExecute()
	return err
}
