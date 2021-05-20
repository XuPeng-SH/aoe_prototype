package memtable

import (
	"aoe/pkg/engine"
	bmgrif "aoe/pkg/engine/buffer/manager/iface"
	imem "aoe/pkg/engine/memtable/base"
	md "aoe/pkg/engine/metadata"
	dops "aoe/pkg/engine/ops/data"
	mops "aoe/pkg/engine/ops/meta"
	todo "aoe/pkg/mock"
	// log "github.com/sirupsen/logrus"
	"sync"
)

type Collection struct {
	ID     uint64
	Opts   *engine.Options
	BufMgr bmgrif.IBufferManager
	mem    struct {
		sync.RWMutex
		MemTables []imem.IMemTable
	}
}

var (
	_ imem.ICollection = (*Collection)(nil)
)

func NewCollection(bmgr bmgrif.IBufferManager, opts *engine.Options, id uint64) imem.ICollection {
	c := &Collection{
		ID:     id,
		Opts:   opts,
		BufMgr: bmgr,
	}
	c.mem.MemTables = make([]imem.IMemTable, 0)
	return c
}

func (c *Collection) onNoBlock() (blk *md.Block, err error) {
	ctx := mops.OpCtx{TableID: c.ID}
	op := mops.NewCreateBlkOp(&ctx, c.Opts.Meta.Info, c.Opts.Meta.Updater)
	op.Push()
	err = op.WaitDone()
	if err != nil {
		return nil, err
	}
	blk = op.GetBlock()
	return blk, nil
}

func (c *Collection) onNoMutableTable() (tbl imem.IMemTable, err error) {
	blk, err := c.onNoBlock()
	if err != nil {
		return nil, err
	}
	tbl = NewMemTable(c.BufMgr, c.Opts, blk)
	c.mem.MemTables = append(c.mem.MemTables, tbl)
	return tbl, err
}

func (c *Collection) Append(ck *todo.Chunk, index *md.LogIndex) (err error) {
	var mut imem.IMemTable
	c.mem.Lock()
	defer c.mem.Unlock()
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
		if mut.IsFull() {
			mut, err = c.onNoMutableTable()
			if err != nil {
				c.Opts.EventListener.BackgroundErrorCB(err)
				return err
			}
			go func() {
				ctx := dops.OpCtx{Collection: c}
				op := dops.NewFlushBlkOp(&ctx, c.Opts.Data.Flusher)
				op.Push()
				op.WaitDone()
			}()
		}
		n, err := mut.Append(ck, offset, index)
		if err != nil {
			return err
		}
		offset += n
		if offset == ck.GetCount() {
			break
		}
		if index.IsApplied() {
			break
		}
		index.Start += n
		index.Count = uint64(0)
	}
	return nil
}

func (c *Collection) FetchImmuTable() imem.IMemTable {
	c.mem.Lock()
	defer c.mem.Unlock()
	if len(c.mem.MemTables) <= 1 {
		return nil
	}
	var immu imem.IMemTable
	immu, c.mem.MemTables = c.mem.MemTables[0], c.mem.MemTables[1:]
	return immu
}
