package db

import (
	e "aoe/pkg/engine"
	bmgrif "aoe/pkg/engine/buffer/manager/iface"
	mtif "aoe/pkg/engine/memtable/base"
	"io"
	"os"
	"sync/atomic"
)

// type Reader interface {
// }
// type Writer interface {
// }

type DB struct {
	Dir  string
	Opts *e.Options

	MemTableMgr     mtif.IManager
	MutableBufMgr   bmgrif.IBufferManager
	TableDataBufMgr bmgrif.IBufferManager

	DataDir  *os.File
	FileLock io.Closer

	Closed  *atomic.Value
	ClosedC chan struct{}
}

// var (
// 	_ Reader = (*DB)(nil)
// 	_ Writer = (*DB)(nil)
// )
