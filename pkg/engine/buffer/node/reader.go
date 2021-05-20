package node

import (
	e "aoe/pkg/engine"
	"aoe/pkg/engine/buffer/node/iface"
	"context"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	// "fmt"
	// "aoe/pkg/engine/layout"
)

const (
	NODE_READER = "NR"
)

func init() {
	e.READER_FACTORY.RegisterBuilder(NODE_READER, &NodeReaderBuilder{})
}

type NodeReaderBuilder struct {
}

func (b *NodeReaderBuilder) Build(wf *e.ReaderFactory, ctx context.Context) e.Reader {
	buffer := ctx.Value("buffer").(iface.INodeBuffer)
	if buffer == nil {
		panic("logic error")
	}
	r := &NodeReader{
		Opts:    wf.Opts,
		Dirname: wf.Dirname,
		Buffer:  buffer,
	}
	return r
}

type NodeReader struct {
	Opts     *e.Options
	Dirname  string
	Buffer   iface.INodeBuffer
	Filename string
}

func (nr *NodeReader) Load() (err error) {
	node := nr.Buffer.GetDataNode()
	id := nr.Buffer.GetID()

	fname := e.MakeFilename(nr.Dirname, e.FTNode, MakeNodeFileName(&id), false)
	dir := filepath.Dir(fname)
	log.Info(dir)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
	}
	if err != nil {
		return err
	}

	w, err := os.OpenFile(fname, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	_, err = w.Read(node.Data)
	if err != nil {
		return err
	}
	nr.Filename = fname
	return err
}
