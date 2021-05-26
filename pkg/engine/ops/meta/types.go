package meta

import (
	e "aoe/pkg/engine"
	md "aoe/pkg/engine/metadata"
	"aoe/pkg/engine/ops"
	// log "github.com/sirupsen/logrus"
)

type OpCtx struct {
	Block *md.Block
	Opts  *e.Options
}

type Op struct {
	ops.Op
	Ctx *OpCtx
}
