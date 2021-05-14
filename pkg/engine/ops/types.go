package ops

import (
	iops "aoe/pkg/engine/ops/base"
	iworker "aoe/pkg/engine/worker/base"
	// log "github.com/sirupsen/logrus"
)

type Op struct {
	Impl   iops.IOpInternal
	ErrorC chan error
	Worker iworker.IOpWorker
	Err    error
	Result interface{}
}
