package ops

import (
	// md "aoe/pkg/metadata"
	"errors"
	log "github.com/sirupsen/logrus"
)

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
	err = op.execute()
	if err != nil {
		return err
	}
	err = op.postExecute()
	return err
}
