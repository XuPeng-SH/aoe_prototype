package engine

import (
	md "aoe/pkg/metadata3"
	"aoe/pkg/metadata3/ops"
)

type Options struct {
	Meta struct {
		Flusher ops.IOpWorker
		Updater ops.IOpWorker
		Conf    *md.Configuration
		Info    *md.MetaInfo
	}

	Data struct {
		Flusher ops.IOpWorker
		Sorter  ops.IOpWorker
	}
}

func (o *Options) FillDefaults() *Options {
	if o == nil {
		o = &Options{}
	}
	if o.Meta.Flusher == nil {
		o.Meta.Flusher = ops.NewOperationWorker()
	}
	if o.Meta.Updater == nil {
		o.Meta.Updater = ops.NewOperationWorker()
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
		o.Data.Flusher = ops.NewOperationWorker()
	}

	if o.Data.Sorter == nil {
		o.Data.Sorter = ops.NewOperationWorker()
	}
	return o
}
