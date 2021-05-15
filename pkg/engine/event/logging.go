package event

import (
	imem "aoe/pkg/engine/memtable/base"
	md "aoe/pkg/engine/metadata"
	log "github.com/sirupsen/logrus"
)

func NewLoggingEventListener() EventListener {
	return EventListener{
		BackgroundErrorCB: func(err error) {
			log.Errorf("BackgroundError %s", err)
		},

		MemTableFullCB: func(table imem.IMemTable) {
			log.Infof("MemTable %d is full", table.GetMeta().GetID())
		},

		FlushBlockBeginCB: func(table imem.IMemTable) {
			log.Infof("MemTable %d begins to flush", table.GetMeta().GetID())
		},

		FlushBlockEndCB: func(table imem.IMemTable) {
			log.Infof("MemTable %d end flush", table.GetMeta().GetID())
		},

		CheckpointStartCB: func(info *md.MetaInfo) {
			log.Infof("Start checkpoint %d", info.CheckPoint)
		},

		CheckpointEndCB: func(info *md.MetaInfo) {
			log.Infof("End checkpoint %d", info.CheckPoint)
		},
	}
}
