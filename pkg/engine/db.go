package engine

import (
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
	Opts *Options

	DataDir  *os.File
	FileLock io.Closer

	Closed  *atomic.Value
	ClosedC chan struct{}
}

// var (
// 	_ Reader = (*DB)(nil)
// 	_ Writer = (*DB)(nil)
// )
