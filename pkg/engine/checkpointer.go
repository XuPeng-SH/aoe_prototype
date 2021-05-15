package engine

import (
	md "aoe/pkg/engine/metadata"
	"errors"
	log "github.com/sirupsen/logrus"
)

type Checkpointer struct {
	Opts    *Options
	Dirname string
}

func NewCheckpointer(opts *Options, dirname string) *Checkpointer {
	ck := &Checkpointer{
		Opts:    opts,
		Dirname: dirname,
	}
	return ck
}

func (ck *Checkpointer) Commit(info *md.MetaInfo) error {
	if info == nil {
		log.Error("nil info")
		return errors.New("nil info")
	}
	fname := MakeFilename(ck.Dirname, FTCheckpoint, info.CheckPoint)
	log.Infof("Commit CheckPoint: %s", fname)
	return nil
}
