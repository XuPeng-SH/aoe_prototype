package engine

import (
	"context"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

var (
	READER_FACTORY = &ReaderFactory{
		Builders: make(map[string]ReaderBuilder),
	}
)

type Reader interface {
	Load() error
}

type ReaderBuilder interface {
	Build(wf *ReaderFactory, ctx context.Context) Reader
}

type ReaderFactory struct {
	Opts     *Options
	Dirname  string
	Builders map[string]ReaderBuilder
}

func (wf *ReaderFactory) Init(opts *Options, dirname string) {
	wf.Opts = opts
	wf.Dirname = dirname
}

func (wf *ReaderFactory) RegisterBuilder(name string, wb ReaderBuilder) {
	_, ok := wf.Builders[name]
	if ok {
		panic(fmt.Sprintf("Duplicate reader %s found", name))
	}
	wf.Builders[name] = wb
}

func (wf *ReaderFactory) MakeReader(name string, ctx context.Context) Reader {
	wb, ok := wf.Builders[name]
	if !ok {
		return nil
	}
	return wb.Build(wf, ctx)
}
