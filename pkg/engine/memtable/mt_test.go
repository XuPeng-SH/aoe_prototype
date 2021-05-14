package memtable

import (
	"aoe/pkg/engine"
	md "aoe/pkg/metadata3"
	"aoe/pkg/metadata3/ops"
	todo "aoe/pkg/mock"
	"github.com/stretchr/testify/assert"
	"testing"
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
	opts.FillDefaults()

	opts.Meta.Updater.Start()

	opCtx := ops.OperationContext{}
	op := ops.NewCreateTableOperation(&opCtx, opts.Meta.Info, opts.Meta.Updater)
	op.Push()
	err := op.WaitDone()
	assert.Nil(t, err)
	tbl := op.GetTable()

	manager := NewManager(opts)
	c0, _ := manager.RegisterCollection(tbl.ID)
	expect_blks := uint64(20)
	insert := todo.NewChunk(expect_blks*opts.Meta.Conf.BlockMaxRows, nil)
	insert.Count = insert.Capacity
	index := &md.LogIndex{
		ID:       uint64(0),
		Capacity: insert.GetCount(),
	}
	err = c0.Append(insert, index)
	assert.Nil(t, err)
	// assert.Equal(t, len(tbl.Segments()), expect_blks/md.Meta.MaxRowCount)
	t.Log(tbl.String())

	opts.Meta.Updater.Stop()
}