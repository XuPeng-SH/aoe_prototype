package memtable

import (
	"aoe/pkg/engine"
	bmgr "aoe/pkg/engine/buffer/manager"
	dio "aoe/pkg/engine/dataio"
	"aoe/pkg/engine/layout"
	"aoe/pkg/engine/layout/table"
	md "aoe/pkg/engine/metadata"
	mops "aoe/pkg/engine/ops/meta"
	w "aoe/pkg/engine/worker"
	todo "aoe/pkg/mock"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

var WORK_DIR = "/tmp/memtable/mt_test"

func init() {
	dio.WRITER_FACTORY.Init(nil, WORK_DIR)
	dio.READER_FACTORY.Init(nil, WORK_DIR)
}

func TestManager(t *testing.T) {
	opts := &engine.Options{}
	manager := NewManager(opts)
	assert.Equal(t, len(manager.CollectionIDs()), 0)
	t0 := uint64(0)
	colDefs := make([]table.IColumnDef, 2)
	t0_data := table.NewTableData(t0, colDefs)

	c0, err := manager.RegisterCollection(t0_data)
	assert.Nil(t, err)
	assert.NotNil(t, c0)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	c00, err := manager.RegisterCollection(t0_data)
	assert.NotNil(t, err)
	assert.Nil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	c00, err = manager.UnregisterCollection(t0 + 1)
	assert.NotNil(t, err)
	assert.Nil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	c00, err = manager.UnregisterCollection(t0)
	assert.Nil(t, err)
	assert.NotNil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 0)
}

func TestCollection(t *testing.T) {
	opts := new(engine.Options)
	// opts.EventListener = e.NewLoggingEventListener()
	dirname := "/tmp"
	opts.FillDefaults(dirname)

	opts.Meta.Updater.Start()
	opts.Meta.Flusher.Start()
	opts.Data.Flusher.Start()
	opts.Data.Sorter.Start()

	opCtx := mops.OpCtx{}
	op := mops.NewCreateTblOp(&opCtx, opts.Meta.Info, opts.Meta.Updater)
	op.Push()
	err := op.WaitDone()
	assert.Nil(t, err)
	tbl := op.GetTable()

	manager := NewManager(opts)
	colDefs := make([]table.IColumnDef, 2)
	t0_data := table.NewTableData(tbl.ID, colDefs)
	c0, _ := manager.RegisterCollection(t0_data)
	blks := uint64(20)
	expect_blks := blks
	batch_size := uint64(4)
	step := expect_blks / batch_size
	var waitgroup sync.WaitGroup
	seq := uint64(0)
	for expect_blks > 0 {
		thisStep := step
		if expect_blks < step {
			thisStep = expect_blks
			expect_blks = 0
		} else {
			expect_blks -= step
		}
		waitgroup.Add(1)
		logid := seq
		seq++
		go func(id uint64, wg *sync.WaitGroup) {
			defer wg.Done()
			insert := todo.NewChunk(thisStep*opts.Meta.Conf.BlockMaxRows, nil)
			insert.Count = insert.Capacity
			index := &md.LogIndex{
				ID:       id,
				Capacity: insert.GetCount(),
			}
			err = c0.Append(insert, index)
			assert.Nil(t, err)
		}(logid, &waitgroup)
	}
	waitgroup.Wait()
	assert.Equal(t, len(tbl.SegmentIDs()), int(blks/opts.Meta.Info.Conf.SegmentMaxBlocks))
	// t.Log(opts.Meta.Info.String())
	time.Sleep(time.Duration(100) * time.Millisecond)

	opts.Meta.Updater.Stop()
	opts.Meta.Flusher.Stop()
	opts.Data.Flusher.Stop()
	opts.Data.Sorter.Stop()
}

func TestContainer(t *testing.T) {
	capacity := uint64(4096)
	flusher := w.NewOpWorker()
	bufMgr := bmgr.NewBufferManager(capacity, flusher)

	baseid := layout.BlockId{}
	step := capacity / 2
	// step := capacity
	con := NewDynamicContainer(bufMgr, baseid, step)
	assert.Equal(t, uint64(0), con.GetCapacity())

	err := con.Allocate()
	assert.Nil(t, err)
	assert.Equal(t, step, con.GetCapacity())
	assert.True(t, con.IsPined())

	id2 := baseid
	id2.BlockID += 1
	con2 := NewDynamicContainer(bufMgr, id2, step)
	assert.NotNil(t, con2)
	err = con2.Allocate()
	assert.Nil(t, err)

	err = con2.Allocate()
	assert.NotNil(t, err)
	assert.Equal(t, step, con2.GetCapacity())

	con.Unpin()
	err = con2.Allocate()
	assert.Nil(t, err)
	assert.Equal(t, step*2, con2.GetCapacity())
	assert.Equal(t, capacity, bufMgr.GetUsage())

	con.Close()
	con2.Close()
	assert.Equal(t, uint64(0), con.GetCapacity())
	assert.Equal(t, uint64(0), con2.GetCapacity())
	assert.Equal(t, capacity, bufMgr.GetCapacity())
}
