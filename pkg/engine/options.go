package engine

import (
	e "aoe/pkg/engine/event"
	md "aoe/pkg/engine/metadata"
	w "aoe/pkg/engine/worker"
	iw "aoe/pkg/engine/worker/base"
	todo "aoe/pkg/mock"
)

type Options struct {
	EventListener e.EventListener

	Mon struct {
		Collector iw.IOpWorker
	}

	Meta struct {
		Flusher      iw.IOpWorker
		Updater      iw.IOpWorker
		Checkpointer *Checkpointer
		Conf         *md.Configuration
		Info         *md.MetaInfo
	}

	Data struct {
		Flusher iw.IOpWorker
		Sorter  iw.IOpWorker
		Writer  todo.DataWriter
	}
}

func (o *Options) FillDefaults(dirname string) *Options {
	if o == nil {
		o = &Options{}
	}
	o.EventListener.FillDefaults()

	if o.Mon.Collector == nil {
		o.Mon.Collector = w.NewOpWorker()
	}

	if o.Meta.Flusher == nil {
		o.Meta.Flusher = w.NewOpWorker()
	}
	if o.Meta.Updater == nil {
		o.Meta.Updater = w.NewOpWorker()
	}
	if o.Meta.Conf == nil {
		o.Meta.Conf = &md.Configuration{
			BlockMaxRows:     md.BLOCK_ROW_COUNT,
			SegmentMaxBlocks: md.SEGMENT_BLOCK_COUNT,
		}
	}
	if o.Meta.Info == nil {
		o.Meta.Info = md.NewMetaInfo(o.Meta.Conf)
	}

	if o.Meta.Checkpointer == nil {
		o.Meta.Checkpointer = NewCheckpointer(o, dirname)
	}

	if o.Data.Writer == nil {
		o.Data.Writer = todo.NewDataWriter()
	}

	if o.Data.Flusher == nil {
		o.Data.Flusher = w.NewOpWorker()
	}

	if o.Data.Sorter == nil {
		o.Data.Sorter = w.NewOpWorker()
	}
	return o
}
