package engine

import (
	md "aoe/pkg/engine/metadata"
	"errors"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
)

type Checkpointer struct {
	Opts    *Options
	Dirname string
	TmpFile string
}

func NewCheckpointer(opts *Options, dirname string) *Checkpointer {
	ck := &Checkpointer{
		Opts:    opts,
		Dirname: dirname,
	}
	return ck
}

func (ck *Checkpointer) PreCommit(info *md.MetaInfo) error {
	if info == nil {
		log.Error("nil info")
		return errors.New("nil info")
	}
	fname := MakeFilename(ck.Dirname, FTCheckpoint, strconv.Itoa(int(info.CheckPoint)), true)
	log.Infof("PreCommit CheckPoint: %s", fname)
	w, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	// log.Infof(info.String())
	err = info.Serialize(w)
	if err != nil {
		return err
	}
	ck.TmpFile = fname
	return nil
}

func (ck *Checkpointer) Commit() error {
	if len(ck.TmpFile) == 0 {
		return errors.New("Cannot Commit checkpoint, should do PreCommit before")
	}
	fname, err := FilenameFromTmpfile(ck.TmpFile)
	if err != nil {
		return err
	}
	log.Infof("Commit CheckPoint: %s", fname)
	err = os.Rename(ck.TmpFile, fname)
	return err
}

func (ck *Checkpointer) Load() error {
	// TODO
	return nil
}
