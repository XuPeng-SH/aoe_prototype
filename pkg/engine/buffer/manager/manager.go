package manager

import (
	buf "aoe/pkg/engine/buffer"
	mgrif "aoe/pkg/engine/buffer/manager/iface"
	"aoe/pkg/engine/buffer/node"
	nif "aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"

	log "github.com/sirupsen/logrus"
)

var (
	_ mgrif.IBufferManager = (*BufferManager)(nil)
)

func NewBufferManager(capacity uint64, evict_q_size ...uint64) mgrif.IBufferManager {

	mgr := &BufferManager{
		IMemoryPool: buf.NewSimpleMemoryPool(capacity),
		Nodes:       make(map[layout.BlockId]nif.INodeHandle),
		EvictHolder: NewSimpleEvictHolder(evict_q_size...),
	}

	return mgr
}

func (mgr *BufferManager) GetPool() buf.IMemoryPool {
	return mgr.IMemoryPool
}

func (mgr *BufferManager) RegisterNode(node_id layout.BlockId) nif.INodeHandle {
	mgr.Lock()
	defer mgr.Unlock()

	handle, ok := mgr.Nodes[node_id]
	if ok {
		if !handle.IsClosed() {
			return handle
		}
	}
	ctx := node.NodeHandleCtx{
		ID:      node_id,
		Manager: mgr,
	}
	handle = node.NewNodeHandle(&ctx)
	mgr.Nodes[node_id] = handle
	return handle
}

// func (mgr *BufferManager) GetUsage() uint64 {
// 	return mgr.IMemoryPool.GetUsage()
// }

// func (mgr *BufferManager) GetCapacity() uint64 {
// 	return mgr.Pool.GetCapacity()
// }

// Temp only can SetCapacity with larger size
func (mgr *BufferManager) SetCapacity(capacity uint64) error {
	mgr.Lock()
	defer mgr.Unlock()
	// if !mgr.makeSpace(0, capacity) {
	// 	panic(fmt.Sprintf("Cannot makeSpace(%d,%d)", 0, capacity))
	// }
	// types.AtomicStore(&(mgr.Capacity), capacity)
	return mgr.SetCapacity(capacity)
}

func (mgr *BufferManager) UnregisterNode(node_id layout.BlockId, can_destroy bool) {
	// if node_id.IsTransientBlock() {
	// PXU TODO
	// return
	// }
	mgr.Lock()
	defer mgr.Unlock()
	delete(mgr.Nodes, node_id)
}

func (mgr *BufferManager) Unpin(handle nif.INodeHandle) {
	handle.Lock()
	defer handle.Unlock()
	if !handle.UnRef() {
		panic("logic error")
	}
	if !handle.HasRef() {
		evict_node := &EvictNode{Handle: handle, Iter: handle.IncIteration()}
		mgr.EvictHolder.Enqueue(evict_node)
	}
}

func (mgr *BufferManager) makePoolNode(capacity uint64) *buf.Node {
	node := mgr.MakeNode(capacity)
	if node != nil {
		return node
	}
	for node == nil {
		// log.Printf("makePoolNode capacity %d now %d", capacity, mgr.GetUsageSize())
		evict_node := mgr.EvictHolder.Dequeue()
		// log.Infof("Evict node %s", evict_node.String())
		if evict_node == nil {
			log.Printf("Cannot get node from queue")
			return nil
		}
		if evict_node.Handle.IsClosed() {
			continue
		}

		if !evict_node.Unloadable(evict_node.Handle) {
			continue
		}

		{
			evict_node.Handle.Lock()
			defer evict_node.Handle.Unload()
			if !evict_node.Unloadable(evict_node.Handle) {
				continue
			}
			if !evict_node.Handle.Unloadable() {
				continue
			}
			evict_node.Handle.Unload()
		}
		node = mgr.MakeNode(capacity)
	}
	return node
}

func (mgr *BufferManager) Pin(handle nif.INodeHandle) nif.IBufferHandle {
	handle.Lock()
	defer handle.Unlock()
	if handle.PrepareLoad() {
		n := mgr.makePoolNode(handle.GetCapacity())
		if n == nil {
			handle.RollbackLoad()
			log.Warnf("Cannot makeSpace(%d,%d)", handle.GetCapacity(), mgr.GetCapacity())
			return nil
		}
		buf := node.NewNodeBuffer(handle.GetID(), n)
		handle.SetBuffer(buf)
		if err := handle.CommitLoad(); err != nil {
			handle.RollbackLoad()
			panic(err.Error())
		}
	}
	handle.Ref()
	return handle.MakeHandle()
}
