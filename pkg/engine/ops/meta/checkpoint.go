package meta

import (
	md "aoe/pkg/engine/metadata"
	iworker "aoe/pkg/engine/worker/base"
	"errors"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

func NewCheckpointOp(ctx *OpCtx, info *md.MetaInfo,
	w iworker.IOpWorker) *CheckpointOp {
	op := &CheckpointOp{}
	op.Op = *NewOp(op, ctx, info, w)
	return op
}

type CheckpointOp struct {
	Op
	// Checkpointer *Checkpointer
}

func (op *CheckpointOp) Execute() (err error) {
	ts := md.NowMicro()
	meta := op.MetaInfo.Copy(ts)
	if meta == nil {
		err = errors.New(fmt.Sprintf("CheckPoint error"))
		return err
	}
	meta.CheckPoint += 1
	// err = op.Checkpointer.Commit(meta)
	return err
	// tmpfile, err :=  op.CheckpointWriter(meta)
	// if err := nil {
	// 	return err
	// }
	// err = CommitCheckpoint(tmpfile)
	return err
}