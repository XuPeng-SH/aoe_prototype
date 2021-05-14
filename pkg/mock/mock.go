package mock

import (
	md "aoe/pkg/metadata3"
	ops "aoe/pkg/metadata3/ops"
)

func NewChunk(capacity uint64, meta *md.Block) *Chunk {
	return nil
}

type Chunk struct {
}

func (c *Chunk) Append(o *Chunk, offset uint64) (n uint64, err error) {
	return n, err
}

func (c *Chunk) Count() uint64 {
	return uint64(0)
}

type DataWriter interface {
	Write(obj interface{}) error
}

var (
	MetaWorker = ops.NewOperationWorker()
)

func init() {
	MetaWorker.Start()
}
