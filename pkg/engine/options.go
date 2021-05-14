package engine

import (
	"aoe/pkg/metadata3/ops"
)

type Options struct {
	Meta struct {
		BlockMaxRows     uint64
		SegmentMaxBlocks uint64
		Worker           ops.IOpWorker
	}
}

func (o *Options) FillDefaults() *Options {
	if o == nil {
		o = &Options{}
	}
	if o.Meta.Worker == nil {
		o.Meta.Worker = ops.NewOperationWorker()
	}
	return o
}
