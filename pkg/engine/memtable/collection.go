package memtable

import (
	"aoe/pkg/engine"
	"aoe/pkg/engine/layout"
	"aoe/pkg/engine/layout/table"
	"aoe/pkg/engine/layout/table/col"
	imem "aoe/pkg/engine/memtable/base"
	md "aoe/pkg/engine/metadata"
	dops "aoe/pkg/engine/ops/data"
	mops "aoe/pkg/engine/ops/meta"
	"aoe/pkg/mock/type/chunk"

	// log "github.com/sirupsen/logrus"
	"sync"
)

type Collection struct {
	ID        uint64
	Opts      *engine.Options
	TableData table.ITableData
	mem       struct {
		sync.RWMutex
		MemTables []imem.IMemTable
		Cursors   []col.IScanCursor
	}
}

var (
	_ imem.ICollection = (*Collection)(nil)
)

func NewCollection(tableData table.ITableData, opts *engine.Options) imem.ICollection {
	c := &Collection{
		ID:        tableData.GetID(),
		Opts:      opts,
		TableData: tableData,
	}
	c.mem.MemTables = make([]imem.IMemTable, 0)
	c.mem.Cursors = make([]col.IScanCursor, len(tableData.GetCollumns()))
	return c
}

func (c *Collection) onNoBlock() (blk *md.Block, newSeg bool, err error) {
	ctx := mops.OpCtx{TableID: c.ID}
	op := mops.NewCreateBlkOp(&ctx, c.Opts.Meta.Info, c.Opts.Meta.Updater)
	op.Push()
	err = op.WaitDone()
	if err != nil {
		return nil, false, err
	}
	blk = op.GetBlock()
	return blk, op.HasNewSegment(), nil
}

func (c *Collection) onNoMutableTable() (tbl imem.IMemTable, err error) {
	blk, newSeg, err := c.onNoBlock()
	if err != nil {
		return nil, err
	}

	columns := make([]col.IColumnBlock, 0)
	for idx, column := range c.TableData.GetCollumns() {
		if newSeg {
			seg_id := layout.ID{
				TableID:   c.ID,
				SegmentID: blk.SegmentID,
			}
			// TODO: All column data modification should be executed by one worker
			_, err = column.RegisterSegment(seg_id)
			if err != nil {
				return nil, err
			}
		}

		blk_id := layout.ID{
			TableID:   blk.TableID,
			SegmentID: blk.SegmentID,
			BlockID:   blk.ID,
		}
		// TODO: All column data modification should be executed by one worker
		colBlk, _ := column.RegisterBlock(c.TableData.GetBufMgr(), blk_id, blk.MaxRowCount)
		columns = append(columns, colBlk)
		c.mem.Cursors[idx] = &col.ScanCursor{}
		colBlk.InitScanCursor(c.mem.Cursors[idx].(*col.ScanCursor))
	}

	tbl = NewMemTable(c.TableData.GetColTypes(), columns, c.mem.Cursors, c.Opts, blk)
	c.mem.MemTables = append(c.mem.MemTables, tbl)
	return tbl, err
}

func (c *Collection) Append(ck *chunk.Chunk, index *md.LogIndex) (err error) {
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
			for _, cursor := range c.mem.Cursors {
				cursor.Close()
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
