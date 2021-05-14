package engine

import (
	md "aoe/pkg/engine/metadata"
	w "aoe/pkg/engine/worker"
	iw "aoe/pkg/engine/worker/base"
)

type Options struct {
	Meta struct {
		Flusher iw.IOpWorker
		Updater iw.IOpWorker
		Conf    *md.Configuration
		Info    *md.MetaInfo
	}

	Data struct {
		Flusher iw.IOpWorker
		Sorter  iw.IOpWorker
	}
}

func (o *Options) FillDefaults() *Options {
	if o == nil {
		o = &Options{}
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

	if o.Data.Flusher == nil {
		o.Data.Flusher = w.NewOpWorker()
	}

	if o.Data.Sorter == nil {
		o.Data.Sorter = w.NewOpWorker()
	}
	return o
}
