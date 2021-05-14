package meta

import (
	md "aoe/pkg/engine/metadata"
	"aoe/pkg/engine/ops"
	// log "github.com/sirupsen/logrus"
)

type OpCtx struct {
	TableID     uint64
	Block       *md.Block
	TmpMetaFile string
}

type Op struct {
	ops.Op
	Ctx      *OpCtx
	MetaInfo *md.MetaInfo
}
