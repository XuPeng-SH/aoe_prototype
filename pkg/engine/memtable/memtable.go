package memtable

import (
	"aoe/pkg/engine"
	"aoe/pkg/engine/ops"
	mops "aoe/pkg/engine/ops/meta"
	util "aoe/pkg/metadata"
	md "aoe/pkg/metadata3"
	todo "aoe/pkg/mock"
	"sync"
)

type IMemTable interface {
	Append(c *todo.Chunk, offset uint64, index *md.LogIndex) (n uint64, err error)
	IsFull() bool
}

type MemTable struct {
	Opts *engine.Options
	util.RefProxy
	sync.RWMutex
	W    todo.DataWriter
	Meta *md.Block
	Data *todo.Chunk
	Full bool
}

var (
	_ IMemTable = (*MemTable)(nil)
)

func NewMemTable(opts *engine.Options, meta *md.Block) IMemTable {
	mt := &MemTable{
		Meta: meta,
		Data: todo.NewChunk(meta.MaxRowCount, meta),
		Full: false,
		Opts: opts,
	}

	return mt
}

func (mt *MemTable) Append(c *todo.Chunk, offset uint64, index *md.LogIndex) (n uint64, err error) {
	mt.Lock()
	defer mt.Unlock()
	n, err = mt.Data.Append(c, offset)
	if err != nil {
		return n, err
	}
	index.Count += n
	mt.Meta.SetIndex(*index)
	if mt.Data.GetCount() == mt.Meta.MaxRowCount {
		mt.Full = true
	}
	return n, err
}

// A flush worker call this Flush API. When a MemTable is ready to flush. It immutable.
// Steps:
// 1. Serialize mt.Data to block_file (dir/$table_id_$segment_id_$block_id.blk)
// 2. Create a UpdateBlockOp and excute it
// 3. Start a checkpoint job
// If crashed before Step 1, all data from last checkpoint will be restored from WAL
// If crashed before Step 2, the untracked block file will be cleanup at startup.
// If crashed before Step 3, same as above.
func (mt *MemTable) Flush() error {
	err := mt.W.Write(mt)
	if err != nil {
		return err
	}
	ctx := ops.OperationContext{Block: mt.Meta}
	op := mops.NewUpdateOperation(&ctx, mt.Opts.Meta.Info, mt.Opts.Meta.Updater)
	op.Push()
	err = op.WaitDone()
	if err != nil {
		return err
	}
	// TODO
	// mt.Listener.Send(DO_CHECKPOINT)
	return nil
}

func (mt *MemTable) IsFull() bool {
	return mt.Full
}
