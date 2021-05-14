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
	_ iw.IOpWorker = (*OpWorker)(nil)
)

type OpWorker struct {
	OpC  chan iops.IOp
	CmdC chan Cmd
	Done bool
}

func NewOpWorker() *OpWorker {
	worker := &OpWorker{
		OpC:  make(chan iops.IOp),
		CmdC: make(chan Cmd),
	}
	return worker
}

func (w *OpWorker) Start() {
	log.Infof("Start OpWorker")
	go func() {
		for !w.Done {
			select {
			case op := <-w.OpC:
				w.onOp(op)
			case cmd := <-w.CmdC:
				w.onCmd(cmd)
			}
		}
	}()
}

func (w *OpWorker) Stop() {
	w.CmdC <- QUIT
}

func (w *OpWorker) SendOp(op iops.IOp) {
	w.OpC <- op
}

func (w *OpWorker) onOp(op iops.IOp) {
	// log.Info("OpWorker: onOp")
	err := op.OnExec()
	op.SetError(err)
}

func (w *OpWorker) onCmd(cmd Cmd) {
	switch cmd {
	case QUIT:
		log.Infof("Quit OpWorker")
		w.Done = true
	default:
		panic(fmt.Sprintf("Unsupported cmd %d", cmd))
	}
}
