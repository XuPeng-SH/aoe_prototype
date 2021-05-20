package iface

import (
	e "aoe/pkg/engine"
	"context"
)

type Reader interface {
	Load() error
}

type Writer interface {
	Flush() error
}

type IReaderFactory interface {
	Init(opts *e.Options, dirname string)
	RegisterBuilder(name string, wb ReaderBuilder)
	MakeReader(name string, ctx context.Context) Reader
	GetOpts() *e.Options
	GetDir() string
}

type IWriterFactory interface {
	Init(opts *e.Options, dirname string)
	RegisterBuilder(name string, wb WriterBuilder)
	MakeWriter(name string, ctx context.Context) Writer
	GetOpts() *e.Options
	GetDir() string
}

type ReaderBuilder interface {
	Build(rf IReaderFactory, ctx context.Context) Reader
}

type WriterBuilder interface {
	Build(wf IWriterFactory, ctx context.Context) Writer
}
