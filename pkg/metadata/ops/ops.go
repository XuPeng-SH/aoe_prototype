package ops

import (
	md "aoe/pkg/metadata"
	"errors"
	log "github.com/sirupsen/logrus"
)

func NewOperation(impl IOperationInternal, ctx *OperationContext,
	handle *md.BucketCacheHandle, w IOpWorker) *Operation {
	op := &Operation{
		Ctx:     ctx,
		Handle:  handle,
		Impl:    impl,
		ResultC: make(chan error),
		Worker:  w,
	}
	return op
}

func (op *Operation) Push() {
	op.Worker.SendOp(op)
}

func (op *Operation) SetError(err error) {
	op.ResultC <- err
}

func (op *Operation) WaitDone() error {
	err := <-op.ResultC
	return err
}

func (op *Operation) preExecute() error {
	if op.Ctx == nil {
		return errors.New("No context specified")
	}
	if op.Handle == nil {
		return errors.New("No snapshot specified")
	}
	return nil
}

func (op *Operation) postExecute() error {
	return nil
}

func (op *Operation) execute() error {
	log.Infof("Execute NewBlockOperation on SS %d", op.Handle.GetVersion())
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
