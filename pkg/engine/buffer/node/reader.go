package node

import (
	e "aoe/pkg/engine"
	"aoe/pkg/engine/buffer/node/iface"
	"context"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
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
	handle := ctx.Value("handle").(iface.INodeHandle)
	if handle == nil {
		panic("logic error")
	}
	var filename string
	fn := ctx.Value("filename")
	if fn == nil {
		id := handle.GetID()
		filename = e.MakeFilename(e.READER_FACTORY.Dirname, e.FTNode, MakeNodeFileName(&id), false)
	} else {
		filename = fmt.Sprintf("%v", fn)
	}
	r := &NodeReader{
		Opts:     wf.Opts,
		Dirname:  wf.Dirname,
		Handle:   handle,
		Filename: filename,
	}
	return r
}

type NodeReader struct {
	Opts     *e.Options
	Dirname  string
	Handle   iface.INodeHandle
	Filename string
}

func (nr *NodeReader) Load() (err error) {
	node := nr.Handle.GetBuffer().GetDataNode()
	dir := filepath.Dir(nr.Filename)
	log.Info(dir)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
	}
	if err != nil {
		return err
	}

	w, err := os.OpenFile(nr.Filename, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	_, err = w.Read(node.Data)
	if err != nil {
		return err
	}
	// nr.Filename = fname
	return err
}
