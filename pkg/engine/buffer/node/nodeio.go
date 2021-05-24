package node

import (
	e "aoe/pkg/engine"
	"aoe/pkg/engine/buffer/node/iface"
	dio "aoe/pkg/engine/dataio"
	ioif "aoe/pkg/engine/dataio/iface"
	"context"
	// log "github.com/sirupsen/logrus"
)

func NewNodeIO(opts *e.Options, ctx context.Context) ioif.IO {
	handle := ctx.Value("handle").(iface.INodeHandle)
	if handle == nil {
		panic("logic error")
	}

	id := handle.GetID()
	filename := e.MakeFilename(dio.WRITER_FACTORY.Dirname, e.FTTransientNode, MakeNodeFileName(&id), false)
	ctx = context.WithValue(ctx, "filename", filename)

	iof := dio.NewIOFactory(dio.WRITER_FACTORY, dio.READER_FACTORY, dio.CLEANER_FACTORY)
	nio := iof.MakeIO(NODE_WRITER, NODE_READER, NODE_CLEANER, ctx)
	return nio
}
