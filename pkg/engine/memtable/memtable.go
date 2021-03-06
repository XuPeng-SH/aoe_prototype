package memtable

import (
	"aoe/pkg/engine"
	ioif "aoe/pkg/engine/dataio/iface"
	"aoe/pkg/engine/layout/table/col"
	imem "aoe/pkg/engine/memtable/base"
	md "aoe/pkg/engine/metadata"
	mops "aoe/pkg/engine/ops/meta"
	util "aoe/pkg/metadata"
	mock "aoe/pkg/mock/type"
	"aoe/pkg/mock/type/chunk"
	"aoe/pkg/mock/type/vector"
	"sync"

	log "github.com/sirupsen/logrus"
)

type MemTable struct {
	Opts *engine.Options
	util.RefProxy
	sync.RWMutex
	WF      ioif.IWriterFactory
	Meta    *md.Block
	Data    *chunk.Chunk
	Full    bool
	Columns []col.IColumnBlock
	Cursors []col.IScanCursor
}

var (
	_ imem.IMemTable = (*MemTable)(nil)
)

func NewMemTable(colTypes []mock.ColType, columnBlocks []col.IColumnBlock,
	cursors []col.IScanCursor, opts *engine.Options, meta *md.Block) imem.IMemTable {
	mt := &MemTable{
		Meta:    meta,
		Full:    false,
		Opts:    opts,
		Columns: columnBlocks,
		Cursors: cursors,
	}
	var vectors []vector.Vector
	for idx, blk := range columnBlocks {
		vec := vector.NewStdVector(colTypes[idx], blk.GetPartRoot().GetBuf())
		vectors = append(vectors, vec)
	}

	mt.Data = &chunk.Chunk{
		Vectors: vectors,
	}

	return mt
}

func (mt *MemTable) InitScanCursors(cursors []interface{}) error {
	for idx, colBlock := range mt.Columns {
		cursor := cursors[idx].(*col.ScanCursor)
		err := colBlock.InitScanCursor(cursor)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mt *MemTable) Append(c *chunk.Chunk, offset uint64, index *md.LogIndex) (n uint64, err error) {
	mt.Lock()
	defer mt.Unlock()
	n, err = mt.Data.Append(c, offset)
	if err != nil {
		return n, err
	}
	index.Count = n
	log.Info(mt.Meta.String())
	log.Info(index.String())
	mt.Meta.SetIndex(*index)
	mt.Meta.Count += n
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
	mt.Opts.EventListener.FlushBlockBeginCB(mt)
	// writer := mt.WF.MakeWriter(buf.SPILL_MEMORY_WRITER_BUILDER, context.TODO())
	// err := mt.W.Write(mt)
	// if err != nil {
	// 	mt.Opts.EventListener.BackgroundErrorCB(err)
	// 	return err
	// }
	ctx := mops.OpCtx{Block: mt.Meta, Opts: mt.Opts}
	op := mops.NewUpdateOp(&ctx)
	op.Push()
	err := op.WaitDone()
	if err != nil {
		mt.Opts.EventListener.BackgroundErrorCB(err)
		return err
	}
	go func() {
		ctx := mops.OpCtx{Opts: mt.Opts}
		op := mops.NewCheckpointOp(&ctx)
		op.Push()
		err := op.WaitDone()
		if err != nil {
			mt.Opts.EventListener.BackgroundErrorCB(err)
		}
	}()
	mt.Opts.EventListener.FlushBlockEndCB(mt)
	return nil
}

func (mt *MemTable) GetMeta() *md.Block {
	return mt.Meta
}

func (mt *MemTable) IsFull() bool {
	return mt.Full
}
