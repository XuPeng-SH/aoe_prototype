package memtable

import (
	"aoe/pkg/engine"
	"aoe/pkg/engine/layout/table"
	imem "aoe/pkg/engine/memtable/base"
	"errors"
	"sync"
)

type Manager struct {
	sync.RWMutex
	Opts        *engine.Options
	Collections map[uint64]imem.ICollection
	TableData   table.ITableData
}

var (
	_ imem.IManager = (*Manager)(nil)
)

func NewManager(opts *engine.Options) imem.IManager {
	m := &Manager{
		Opts:        opts,
		Collections: make(map[uint64]imem.ICollection),
	}
	return m
}

func (m *Manager) CollectionIDs() map[uint64]uint64 {
	ids := make(map[uint64]uint64)
	for k, _ := range m.Collections {
		ids[k] = k
	}
	return ids
}

func (m *Manager) GetCollection(id uint64) imem.ICollection {
	m.RLock()
	defer m.RLock()
	c, ok := m.Collections[id]
	if !ok {
		return nil
	}
	return c
}

func (m *Manager) RegisterCollection(td interface{}) (c imem.ICollection, err error) {
	m.Lock()
	defer m.Unlock()
	tableData := td.(table.ITableData)
	_, ok := m.Collections[tableData.GetID()]
	if ok {
		return nil, errors.New("logic error")
	}
	c = NewCollection(tableData, m.Opts)
	m.Collections[tableData.GetID()] = c
	return c, err
}

func (m *Manager) UnregisterCollection(id uint64) (c imem.ICollection, err error) {
	m.Lock()
	defer m.Unlock()
	c, ok := m.Collections[id]
	if ok {
		delete(m.Collections, id)
	} else {
		return nil, errors.New("logic error")
	}
	return c, err
}
