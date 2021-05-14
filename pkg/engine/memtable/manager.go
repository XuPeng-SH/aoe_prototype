package memtable

import (
	"aoe/pkg/engine"
	"errors"
	"sync"
)

type IManager interface {
	GetCollection(id uint64) ICollection
	RegisterCollection(id uint64) (c ICollection, err error)
	UnregisterCollection(id uint64) (c ICollection, err error)
}

type Manager struct {
	sync.RWMutex
	Opts        *engine.Options
	Collections map[uint64]ICollection
}

var (
	_ IManager = (*Manager)(nil)
)

func NewManager(opts *engine.Options) IManager {
	m := &Manager{
		Opts: opts,
	}
	return m
}

func (m *Manager) GetCollection(id uint64) ICollection {
	m.RLock()
	defer m.RLock()
	c, ok := m.Collections[id]
	if !ok {
		return nil
	}
	return c
}

func (m *Manager) RegisterCollection(id uint64) (c ICollection, err error) {
	m.Lock()
	defer m.Unlock()
	c, ok := m.Collections[id]
	if ok {
		return nil, errors.New("logic error")
	}
	c = NewCollection(m.Opts, id)
	m.Collections[id] = c
	return c, err
}

func (m *Manager) UnregisterCollection(id uint64) (c ICollection, err error) {
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
