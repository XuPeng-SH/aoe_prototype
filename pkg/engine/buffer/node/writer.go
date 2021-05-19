package node

import (
	e "aoe/pkg/engine"
	"aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

const (
	NODE_WRITER = "NW"
)

func init() {
	e.WRITER_FACTORY.RegisterBuilder(NODE_WRITER, &NodeWriterBuilder{})
}

type NodeWriterBuilder struct {
}

func (b *NodeWriterBuilder) Build(wf *e.WriterFactory, ctx context.Context) e.Writer {
	buffer := ctx.Value("buffer").(iface.INodeBuffer)
	if buffer == nil {
		panic("logic error")
	}
	w := &NodeWriter{
		Opts:    wf.Opts,
		Dirname: wf.Dirname,
		Buffer:  buffer,
	}
	return w
}

type NodeWriter struct {
	Opts    *e.Options
	Dirname string
	Buffer  iface.INodeBuffer
}

func MakeNodeFileName(id *layout.BlockId) string {
	return fmt.Sprintf("%d_%d_%d_%d", id.TableID, id.SegmentID, id.BlockID, id.PartID)
}

func (sw *NodeWriter) Flush() (err error) {
	node := sw.Buffer.GetDataNode()
	id := sw.Buffer.GetID()

	fname := e.MakeFilename(sw.Dirname, e.FTNode, MakeNodeFileName(&id), false)
	dir := filepath.Dir(fname)
	log.Info(dir)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	}

	w, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	_, err = w.Write(node.Data)
	if err != nil {
		return err
	}
	return err
}
