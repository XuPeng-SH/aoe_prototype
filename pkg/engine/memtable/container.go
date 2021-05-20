package memtable

import (
	bmgrif "aoe/pkg/engine/buffer/manager/iface"
	nif "aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	"errors"
	"fmt"
	// log "github.com/sirupsen/logrus"
	"sync"
)

type Container interface {
	GetCapacity() uint64
	Allocate() error
	Pin() error
	Unpin()
	Close() error
	IsPined() bool
}

type StaticContainer struct {
	Nodes map[layout.BlockId]nif.INodeHandle
}

type DynamicContainer struct {
	sync.RWMutex
	BufMgr   bmgrif.IBufferManager
	StepSize uint64
	BaseID   layout.BlockId
	Nodes    map[layout.BlockId]nif.INodeHandle
	Handles  map[layout.BlockId]nif.IBufferHandle
	Pined    bool
}

func NewDynamicContainer(bmgr bmgrif.IBufferManager, id layout.BlockId, step uint64) Container {
	con := &DynamicContainer{
		BaseID:   id,
		BufMgr:   bmgr,
		StepSize: step,
		Nodes:    make(map[layout.BlockId]nif.INodeHandle),
		Handles:  make(map[layout.BlockId]nif.IBufferHandle),
		Pined:    true,
	}
	return con
}

func (con *DynamicContainer) IsPined() bool {
	con.RLock()
	defer con.RUnlock()
	return con.Pined
}

func (con *DynamicContainer) Allocate() error {
	con.Lock()
	defer con.Unlock()
	if !con.Pined {
		panic("logic error")
	}
	id := con.BaseID
	id.PartID = uint16(len(con.Nodes))
	node := con.BufMgr.RegisterSpillableNode(con.StepSize, id)
	if node == nil {
		return errors.New(fmt.Sprintf("Cannot allocate %d from buffer manager", con.StepSize))
	}
	handle := con.BufMgr.Pin(node)
	if handle == nil {
		node.Close()
		return errors.New(fmt.Sprintf("Cannot pin node %v", id))
	}
	con.Nodes[id] = node
	con.Handles[id] = handle
	return nil
}

func (con *DynamicContainer) GetCapacity() uint64 {
	con.RLock()
	defer con.RUnlock()
	return uint64(int(con.StepSize) * len(con.Nodes))
}

func (con *DynamicContainer) Pin() error {
	con.Lock()
	defer con.Unlock()
	if con.Pined {
		return nil
	}
	for id, n := range con.Nodes {
		h := con.BufMgr.Pin(n)
		if h == nil {
			con.Handles = make(map[layout.BlockId]nif.IBufferHandle)
			return errors.New(fmt.Sprintf("Cannot pin node %v", id))
		}
		con.Handles[id] = h
	}
	con.Pined = true
	return nil
}

func (con *DynamicContainer) Unpin() {
	con.Lock()
	defer con.Unlock()
	if !con.Pined {
		return
	}
	for _, h := range con.Handles {
		err := h.Close()
		if err != nil {
			panic(fmt.Sprintf("logic error: %v", err))
		}
	}
	con.Handles = make(map[layout.BlockId]nif.IBufferHandle)
	con.Pined = false
	return
}

func (con *DynamicContainer) Close() error {
	con.Lock()
	defer con.Unlock()
	for _, h := range con.Handles {
		h.Close()
	}
	con.Handles = make(map[layout.BlockId]nif.IBufferHandle)
	for _, n := range con.Nodes {
		n.Close()
	}
	con.Nodes = make(map[layout.BlockId]nif.INodeHandle)
	return nil
}
