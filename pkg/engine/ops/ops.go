package ops

import (
	md "aoe/pkg/engine/metadata"
	iops "aoe/pkg/engine/ops/base"
	iworker "aoe/pkg/engine/worker/base"
	"errors"
	// log "github.com/sirupsen/logrus"
)

func NewOp(impl iops.IOpInternal, ctx *OpCtx,
	info *md.MetaInfo, w iworker.IOpWorker) *Op {
	op := &Op{
		Ctx:      ctx,
		MetaInfo: info,
		Impl:     impl,
		ErrorC:   make(chan error),
		Worker:   w,
	}
	return op
}

func (op *Op) Push() {
	op.Worker.SendOp(op)
}

func (op *Op) SetError(err error) {
	op.Err = err
	op.ErrorC <- err
}

func (op *Op) WaitDone() error {
	err := <-op.ErrorC
	return err
}

func (op *Op) PreExecute() error {
	if op.Ctx == nil {
		return errors.New("No context specified")
	}
	return nil
}

func (op *Op) PostExecute() error {
	return nil
}

func (op *Op) Execute() error {
	return nil
}

func (op *Op) OnExecute() error {
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
