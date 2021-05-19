package engine

import (
	"context"
	"fmt"
	"io"
	// log "github.com/sirupsen/logrus"
)

var (
	WRITER_FACTORY = &WriterFactory{
		Builders: make(map[string]WriterBuilder),
	}
)

type WriterBuilder interface {
	Build(wf *WriterFactory, ctx context.Context) io.Writer
}

type WriterFactory struct {
	Opts     *Options
	Dirname  string
	Builders map[string]WriterBuilder
}

func (wf *WriterFactory) RegisterBuilder(name string, wb WriterBuilder) {
	_, ok := wf.Builders[name]
	if ok {
		panic(fmt.Sprintf("Duplicate write %s found", name))
	}
	wf.Builders[name] = wb
}

func (wf *WriterFactory) MakeWriter(name string, ctx context.Context) io.Writer {
	wb, ok := wf.Builders[name]
	if !ok {
		return nil
	}
	return wb.Build(wf, ctx)
}
