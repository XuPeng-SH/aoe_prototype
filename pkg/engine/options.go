package engine

type Options struct {
	BlockMaxRows     uint64
	SegmentMaxBlocks uint64
}

func (o *Options) FillDefaults() *Options {
	if o == nil {
		o = &Options{}
	}
	return o
}
