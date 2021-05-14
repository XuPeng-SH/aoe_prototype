package event

import (
	imem "aoe/pkg/engine/memtable/base"
	md "aoe/pkg/engine/metadata"
	log "github.com/sirupsen/logrus"
)

type EventListener struct {
	BackgroundErrorCB func(error)
	MemTableFullCB    func(imem.IMemTable)
	FlushBlockBeginCB func(imem.IMemTable)
	FlushBlockEndCB   func(imem.IMemTable)
	CheckpointStartCB func(*md.MetaInfo)
	CheckpointEndCB   func(*md.MetaInfo)
}

func (l *EventListener) FillDefaults() {
	if l.BackgroundErrorCB == nil {
		l.BackgroundErrorCB = func(err error) {
			log.Errorf("BackgroundError %s", err)
		}
	}

	if l.MemTableFullCB == nil {
		l.MemTableFullCB = func(table imem.IMemTable) {}
	}

	if l.FlushBlockBeginCB == nil {
		l.FlushBlockBeginCB = func(table imem.IMemTable) {}
	}

	if l.FlushBlockEndCB == nil {
		l.FlushBlockEndCB = func(table imem.IMemTable) {}
	}

	if l.CheckpointStartCB == nil {
		l.CheckpointStartCB = func(info *md.MetaInfo) {}
	}

	if l.CheckpointEndCB == nil {
		l.CheckpointStartCB = func(info *md.MetaInfo) {}
	}
}
