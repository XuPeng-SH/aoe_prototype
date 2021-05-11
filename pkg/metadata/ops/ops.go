package ops

import (
	md "aoe/pkg/metadata"
	"errors"
	log "github.com/sirupsen/logrus"
)

func NewOperation(impl IOperation, ctx *OperationContext, handle *md.BucketCacheHandle) *Operation {
	op := &Operation{
		Ctx:    ctx,
		Handle: handle,
		Impl:   impl,
	}
	return op
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
	err := op.Impl.preExecute()
	if err != nil {
		return err
	}
	err = op.Impl.execute()
	if err != nil {
		return err
	}
	err = op.Impl.postExecute()
	return err
}
