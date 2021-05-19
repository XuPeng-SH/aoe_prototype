package buf

import (
	e "aoe/pkg/engine"
	"aoe/pkg/engine/layout"
	"context"
	"fmt"
	"io"
	"os"
)

const (
	SPILL_WRITER = "SW"
)

func init() {
	e.WRITER_FACTORY.RegisterBuilder(SPILL_WRITER, &SpillWriterBuilder{})
}

type SpillWriterBuilder struct {
}

func (b *SpillWriterBuilder) Build(wf *e.WriterFactory, ctx context.Context) io.Writer {
	id := ctx.Value("id").(*layout.BlockId)
	if id == nil || !id.IsTransient() {
		panic("logic error")
	}
	w := &SpillWriter{
		Opts:    wf.Opts,
		Dirname: wf.Dirname,
		ID:      *id,
	}
	return w
}

type SpillWriter struct {
	Opts    *e.Options
	Dirname string
	ID      layout.BlockId
}

func MakeSpillFileName(id *layout.BlockId) string {
	return fmt.Sprintf("%d_%d_%d_%d", id.TableID, id.SegmentID, id.BlockID, id.PartID)
}

func (sw *SpillWriter) Write(data []byte) (n int, err error) {
	// log.Infof("PreCommit CheckPoint: %s", fname)
	fname := e.MakeFilename(sw.Dirname, e.FTSpillMemory, MakeSpillFileName(&sw.ID), false)
	w, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return n, err
	}
	n, err = w.Write(data)
	if err != nil {
		return n, err
	}
	return n, err
}
