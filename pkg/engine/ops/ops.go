package ops

import (
	iops "aoe/pkg/engine/ops/base"
	iworker "aoe/pkg/engine/worker/base"
	md "aoe/pkg/metadata3"
	"errors"
	// log "github.com/sirupsen/logrus"
)

func NewOperation(impl iops.IOperationInternal, ctx *OperationContext,
	info *md.MetaInfo, w iworker.IOpWorker) *Operation {
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

func (op *Operation) PreExecute() error {
	if op.Ctx == nil {
		return errors.New("No context specified")
	}
	return nil
}

func (op *Operation) PostExecute() error {
	return nil
}

func (op *Operation) Execute() error {
	return nil
}

func (op *Operation) OnExecute() error {
	err := op.PreExecute()
	if err != nil {
		return err
	}
	err = op.Impl.PreExecute()
	if err != nil {
		return err
	}
	err = op.Impl.Execute()
	if err != nil {
		return err
	}
	err = op.PostExecute()
	if err != nil {
		return err
	}
	err = op.Impl.PostExecute()
	return err
}
