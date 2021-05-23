package base

import (
	// "aoe/pkg/engine/layout/table"
	md "aoe/pkg/engine/metadata"
	"aoe/pkg/mock/type/chunk"
)

type IMemTable interface {
	Append(c *chunk.Chunk, offset uint64, index *md.LogIndex) (n uint64, err error)
	IsFull() bool
	Flush() error
	GetMeta() *md.Block
	Unpin()
}

type ICollection interface {
	Append(ck *chunk.Chunk, index *md.LogIndex) (err error)
	FetchImmuTable() IMemTable
}

type IManager interface {
	GetCollection(id uint64) ICollection
	RegisterCollection(interface{}) (c ICollection, err error)
	UnregisterCollection(id uint64) (c ICollection, err error)
	CollectionIDs() map[uint64]uint64
}
