package memtable

import (
	"aoe/pkg/engine"
	md "aoe/pkg/metadata3"
	ops "aoe/pkg/metadata3/ops"
	todo "aoe/pkg/mock"
	"sync"
)

type ICollection interface {
	Append(ck *todo.Chunk, index *md.LogIndex) (err error)
}

type Collection struct {
	ID   uint64
	Opts *engine.Options
	mem  struct {
		sync.RWMutex
		MemTables []IMemTable
	}
}

var (
	_ ICollection = (*Collection)(nil)
)

func NewCollection(opts *engine.Options, id uint64) ICollection {
	c := &Collection{
		ID:   id,
		Opts: opts,
	}
	c.mem.MemTables = make([]IMemTable, 0)
	return c
}

func (c *Collection) onNoBlock() (blk *md.Block, err error) {
	ctx := ops.OperationContext{TableID: c.ID}
	op := ops.NewCreateBlockOperation(&ctx, &md.Meta, todo.MetaWorker)
	op.Push()
	err = op.WaitDone()
	if err != nil {
		return nil, err
	}
	blk = op.GetBlock()
	return blk, nil
}

func (c *Collection) onNoMutableTable() (tbl IMemTable, err error) {
	blk, err := c.onNoBlock()
	if err != nil {
		return nil, err
	}
	tbl = NewMemTable(blk)
	return tbl, err
}

func (c *Collection) Append(ck *todo.Chunk, index *md.LogIndex) (err error) {
	var mut IMemTable
	c.mem.Lock()
	size := len(c.mem.MemTables)
	if size == 0 {
		mut, err = c.onNoMutableTable()
		if err != nil {
			return err
		}
	} else {
		mut = c.mem.MemTables[size-1]
	}
	offset := uint64(0)
	for {
		n, err := mut.Append(ck, offset, index)
		if err != nil {
			return err
		}
		offset += n
		if offset == ck.Count() {
			break
		}
		if mut.IsFull() {
			mut, err = c.onNoMutableTable()
			if err != nil {
				return err
			}
		}
	}
	c.mem.Unlock()
	return nil
}

func (c *Collection) FetchImmuTable() IMemTable {
	c.mem.Lock()
	defer c.mem.Unlock()
	if len(c.mem.MemTables) <= 1 {
		return nil
	}
	var immu IMemTable
	immu, c.mem.MemTables = c.mem.MemTables[0], c.mem.MemTables[1:]
	return immu
}
