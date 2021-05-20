package node

import (
	e "aoe/pkg/engine"
	"aoe/pkg/engine/buffer/node/iface"
	dio "aoe/pkg/engine/dataio"
	ioif "aoe/pkg/engine/dataio/iface"
	"context"
	// log "github.com/sirupsen/logrus"
)

type NodeIO struct {
	ioif.Writer
	ioif.Reader
	ioif.Cleaner
}

func NewNodeIO(opts *e.Options, ctx context.Context) ioif.IO {
	handle := ctx.Value("handle").(iface.INodeHandle)
	if handle == nil {
		panic("logic error")
	}

	id := handle.GetID()
	filename := e.MakeFilename(dio.WRITER_FACTORY.Dirname, e.FTNode, MakeNodeFileName(&id), false)
	ctx = context.WithValue(ctx, "filename", filename)

	w := dio.WRITER_FACTORY.MakeWriter(NODE_WRITER, ctx)
	r := dio.READER_FACTORY.MakeReader(NODE_READER, ctx)
	c := dio.CLEANER_FACTORY.MakeCleaner(NODE_CLEANER, ctx)
	nio := &NodeIO{
		Writer:  w,
		Reader:  r,
		Cleaner: c,
	}
	return nio
}
