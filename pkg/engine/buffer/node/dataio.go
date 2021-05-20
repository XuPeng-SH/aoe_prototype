package node

import (
	e "aoe/pkg/engine"
	"aoe/pkg/engine/buffer/node/iface"
	dio "aoe/pkg/engine/dataio"
	ioif "aoe/pkg/engine/dataio/iface"
	"context"
	log "github.com/sirupsen/logrus"
	"os"
)

type emptyCleaner int

func (*emptyCleaner) Clean() error {
	return nil
}

var (
	cleaner = new(emptyCleaner)
)

type Cleaner interface {
	Clean() error
}

type NodeCleaner struct {
	Filename string
}

func NewNodeCleaner(filename string) Cleaner {
	nc := &NodeCleaner{
		Filename: filename,
	}
	return nc
}

func (nc *NodeCleaner) Clean() error {
	log.Infof("NodeCleaner removing %s", nc.Filename)
	return os.Remove(nc.Filename)
}

type IO interface {
	ioif.Writer
	ioif.Reader
	Cleaner
}

type NodeIO struct {
	ioif.Writer
	ioif.Reader
	Cleaner
}

func NewNodeIO(opts *e.Options, ctx context.Context) IO {
	handle := ctx.Value("handle").(iface.INodeHandle)
	if handle == nil {
		panic("logic error")
	}

	id := handle.GetID()
	filename := e.MakeFilename(dio.WRITER_FACTORY.Dirname, e.FTNode, MakeNodeFileName(&id), false)
	ctx = context.WithValue(ctx, "filename", filename)

	w := dio.WRITER_FACTORY.MakeWriter(NODE_WRITER, ctx)
	r := dio.READER_FACTORY.MakeReader(NODE_READER, ctx)
	c := NewNodeCleaner(filename)
	nio := &NodeIO{
		Writer:  w,
		Reader:  r,
		Cleaner: c,
	}
	return nio
}
