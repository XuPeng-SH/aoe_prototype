package ops

import (
	iops "aoe/pkg/engine/ops/base"
	iw "aoe/pkg/engine/worker/base"
	"fmt"
	log "github.com/sirupsen/logrus"
)

type Cmd = uint8

const (
	QUIT Cmd = iota
)

var (
	_ iw.IOpWorker = (*OperationWorker)(nil)
)

type OperationWorker struct {
	OperationC chan iops.IOperation
	CmdC       chan Cmd
	Done       bool
}

func NewOperationWorker() *OperationWorker {
	worker := &OperationWorker{
		OperationC: make(chan iops.IOperation),
		CmdC:       make(chan Cmd),
	}
	return worker
}

func (w *OperationWorker) Start() {
	log.Infof("Start OpWorker")
	go func() {
		for !w.Done {
			select {
			case op := <-w.OperationC:
				w.onOperation(op)
			case cmd := <-w.CmdC:
				w.onCmd(cmd)
			}
		}
	}()
}

func (w *OperationWorker) Stop() {
	w.CmdC <- QUIT
}

func (w *OperationWorker) SendOp(op iops.IOperation) {
	w.OperationC <- op
}

func (w *OperationWorker) onOperation(op iops.IOperation) {
	// log.Info("OpWorker: onOperation")
	err := op.OnExecute()
	op.SetError(err)
}

func (w *OperationWorker) onCmd(cmd Cmd) {
	switch cmd {
	case QUIT:
		log.Infof("Quit OpWorker")
		w.Done = true
	default:
		panic(fmt.Sprintf("Unsupported cmd %d", cmd))
	}
}
