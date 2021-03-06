package manager

import (
	buf "aoe/pkg/engine/buffer"
	mgrif "aoe/pkg/engine/buffer/manager/iface"
	"aoe/pkg/engine/buffer/node"
	nif "aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	ldio "aoe/pkg/engine/layout/dataio"
	iw "aoe/pkg/engine/worker/base"
	"fmt"

	log "github.com/sirupsen/logrus"
)

var (
	_ mgrif.IBufferManager = (*BufferManager)(nil)
)

func NewBufferManager(capacity uint64, flusher iw.IOpWorker, evict_ctx ...interface{}) mgrif.IBufferManager {
	mgr := &BufferManager{
		IMemoryPool: buf.NewSimpleMemoryPool(capacity),
		Nodes:       make(map[layout.ID]nif.INodeHandle),
		EvictHolder: NewSimpleEvictHolder(evict_ctx...),
		TransientID: *layout.NewTransientID(),
		Flusher:     flusher,
	}

	return mgr
}

func (mgr *BufferManager) NodeCount() int {
	mgr.RLock()
	defer mgr.RUnlock()
	return len(mgr.Nodes)
}

func (mgr *BufferManager) String() string {
	mgr.RLock()
	defer mgr.RUnlock()
	s := fmt.Sprintf("BMgr[Cap:%d, Usage:%d, Nodes:%d]:\n", mgr.GetCapacity(), mgr.GetUsage(), len(mgr.Nodes))
	var mapped = map[uint64]map[uint64][]layout.ID{}
	for k, _ := range mgr.Nodes {
		_, ok := mapped[k.TableID]
		if !ok {
			mapped[k.TableID] = make(map[uint64][]layout.ID)
		}
		l := mapped[k.TableID][k.SegmentID]
		l = append(l, k)
		mapped[k.TableID][k.SegmentID] = l
	}
	for tbID, segMap := range mapped {
		s += fmt.Sprintf("Table %d Nodes %d {\n", tbID, len(segMap))
		for segID, ids := range segMap {
			s += fmt.Sprintf("  Segment %d Nodes %d {\n", segID, len(ids))
			for _, id := range ids {
				s += fmt.Sprintf("    Block-%d-Part-%d [%d] (%d)\n", id.BlockID, id.PartID, id.Iter, mgr.Nodes[id].GetState())
			}
			s += "  }\n"
		}
		s += "}"
	}
	return s
}

func (mgr *BufferManager) RegisterMemory(capacity uint64, spillable bool) nif.INodeHandle {
	pNode := mgr.makePoolNode(capacity)
	if pNode == nil {
		return nil
	}
	transient_id := mgr.TransientID.Next()
	ctx := node.NodeHandleCtx{
		ID:        *transient_id,
		Manager:   mgr,
		Buff:      node.NewNodeBuffer(*transient_id, pNode),
		Spillable: spillable,
	}
	handle := node.NewNodeHandle(&ctx)
	return handle
}

func (mgr *BufferManager) RegisterSpillableNode(capacity uint64, node_id layout.ID) nif.INodeHandle {
	// log.Infof("RegisterSpillableNode %s", node_id.String())
	{
		mgr.RLock()
		handle, ok := mgr.Nodes[node_id]
		if ok {
			if !handle.IsClosed() {
				mgr.RUnlock()
				return handle
			}
		}
		mgr.RUnlock()
	}

	pNode := mgr.makePoolNode(capacity)
	if pNode == nil {
		return nil
	}
	ctx := node.NodeHandleCtx{
		ID:        node_id,
		Manager:   mgr,
		Buff:      node.NewNodeBuffer(node_id, pNode),
		Spillable: true,
	}
	handle := node.NewNodeHandle(&ctx)

	mgr.Lock()
	defer mgr.Unlock()
	h, ok := mgr.Nodes[node_id]
	if ok {
		if !h.IsClosed() {
			go func() { mgr.FreeNode(pNode) }()
			return h
		}
	}

	mgr.Nodes[node_id] = handle
	return handle
}

func (mgr *BufferManager) RegisterNode(capacity uint64, node_id layout.ID, segFile interface{}) nif.INodeHandle {
	mgr.Lock()
	defer mgr.Unlock()
	// log.Infof("RegisterNode %s", node_id.String())
	sf := segFile.(ldio.IColSegmentFile)

	handle, ok := mgr.Nodes[node_id]
	if ok {
		if !handle.IsClosed() {
			return handle
		}
	}
	ctx := node.NodeHandleCtx{
		ID:          node_id,
		Manager:     mgr,
		Size:        capacity,
		Spillable:   false,
		SegmentFile: sf,
	}
	handle = node.NewNodeHandle(&ctx)
	mgr.Nodes[node_id] = handle
	return handle
}

func (mgr *BufferManager) UnregisterNode(h nif.INodeHandle) {
	node_id := h.GetID()
	// log.Infof("UnRegisterNode %s", node_id.String())
	if h.IsSpillable() {
		if node_id.IsTransient() {
			h.Clean()
			return
		} else {
			mgr.Lock()
			delete(mgr.Nodes, node_id)
			h.Clean()
			mgr.Unlock()
			return
		}
	}
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
			if !evict_node.Unloadable(evict_node.Handle) {
				continue
			}
			if !evict_node.Handle.Unloadable() {
				continue
			}
			evict_node.Handle.Unload()
			evict_node.Handle.Unlock()
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
