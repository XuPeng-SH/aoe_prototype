package base

import (
	// "aoe/pkg/engine/layout/table"
	md "aoe/pkg/engine/metadata"
	todo "aoe/pkg/mock"
)

type IMemTable interface {
	Append(c *todo.Chunk, offset uint64, index *md.LogIndex) (n uint64, err error)
	IsFull() bool
	Flush() error
	GetMeta() *md.Block
}

type ICollection interface {
	Append(ck *todo.Chunk, index *md.LogIndex) (err error)
	FetchImmuTable() IMemTable
}

type IManager interface {
	GetCollection(id uint64) ICollection
	RegisterCollection(interface{}) (c ICollection, err error)
	UnregisterCollection(id uint64) (c ICollection, err error)
	CollectionIDs() map[uint64]uint64
}
