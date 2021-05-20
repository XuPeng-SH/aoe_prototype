package engine

import (
	"context"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

var (
	WRITER_FACTORY = &WriterFactory{
		Builders: make(map[string]WriterBuilder),
	}
)

type Writer interface {
	Flush() error
}

type WriterBuilder interface {
	Build(wf *WriterFactory, ctx context.Context) Writer
}

type WriterFactory struct {
	Opts     *Options
	Dirname  string
	Builders map[string]WriterBuilder
}

func (wf *WriterFactory) Init(opts *Options, dirname string) {
	wf.Opts = opts
	wf.Dirname = dirname
}

func (wf *WriterFactory) RegisterBuilder(name string, wb WriterBuilder) {
	_, ok := wf.Builders[name]
	if ok {
		panic(fmt.Sprintf("Duplicate write %s found", name))
	}
	wf.Builders[name] = wb
}

func (wf *WriterFactory) MakeWriter(name string, ctx context.Context) Writer {
	wb, ok := wf.Builders[name]
	if !ok {
		return nil
	}
	return wb.Build(wf, ctx)
}
