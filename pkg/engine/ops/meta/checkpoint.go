package meta

import (
	"aoe/pkg/engine"
	md "aoe/pkg/engine/metadata"
	iworker "aoe/pkg/engine/worker/base"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
)

func NewCheckpointOp(ckpointer *engine.Checkpointer, ctx *OpCtx, info *md.MetaInfo,
	w iworker.IOpWorker) *CheckpointOp {
	op := &CheckpointOp{Checkpointer: ckpointer}
	op.Op = *NewOp(op, ctx, info, w)
	return op
}

type CheckpointOp struct {
	Op
	Checkpointer *engine.Checkpointer
}

func (op *CheckpointOp) Execute() (err error) {
	ts := md.NowMicro()
	meta := op.MetaInfo.Copy(ts)
	if meta == nil {
		errMsg := fmt.Sprintf("CheckPoint error")
		log.Error(errMsg)
		err = errors.New(errMsg)
		return err
	}
	meta.CheckPoint += 1
	err = op.Checkpointer.PreCommit(meta)
	if err != nil {
		return err
	}
	err = op.Checkpointer.Commit()
	if err != nil {
		return err
	}
	err = op.MetaInfo.UpdateCheckpoint(meta.CheckPoint)
	if err != nil {
		panic(err)
	}

	return err
}
