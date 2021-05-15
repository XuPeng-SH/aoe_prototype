package memtable

import (
	"aoe/pkg/engine"
	// e "aoe/pkg/engine/event"
	md "aoe/pkg/engine/metadata"
	mops "aoe/pkg/engine/ops/meta"
	todo "aoe/pkg/mock"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestManager(t *testing.T) {
	opts := &engine.Options{}
	manager := NewManager(opts)
	assert.Equal(t, len(manager.CollectionIDs()), 0)
	c0, err := manager.RegisterCollection(0)
	assert.Nil(t, err)
	assert.NotNil(t, c0)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	c00, err := manager.RegisterCollection(0)
	assert.NotNil(t, err)
	assert.Nil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	c00, err = manager.UnregisterCollection(1)
	assert.NotNil(t, err)
	assert.Nil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	c00, err = manager.UnregisterCollection(0)
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
	c0, _ := manager.RegisterCollection(tbl.ID)
	blks := uint64(20)
	expect_blks := blks
	batch_size := uint64(4)
	step := expect_blks / batch_size
	var waitgroup sync.WaitGroup
	for expect_blks > 0 {
		thisStep := step
		if expect_blks < step {
			thisStep = expect_blks
			expect_blks = 0
		} else {
			expect_blks -= step
		}
		waitgroup.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			insert := todo.NewChunk(thisStep*opts.Meta.Conf.BlockMaxRows, nil)
			insert.Count = insert.Capacity
			index := &md.LogIndex{
				ID:       uint64(0),
				Capacity: insert.GetCount(),
			}
			err = c0.Append(insert, index)
			assert.Nil(t, err)
		}(&waitgroup)
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
