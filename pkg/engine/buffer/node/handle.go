package node

import (
	buf "aoe/pkg/engine/buffer"
	mgrif "aoe/pkg/engine/buffer/manager/iface"
	nif "aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	"errors"
	"sync/atomic"
)

func NewNodeHandle(ctx *NodeHandleCtx) nif.INodeHandle {
	size := ctx.Size
	state := nif.NODE_UNLOAD
	if ctx.Buff != nil {
		size = ctx.Buff.GetCapacity()
		state = nif.NODE_LOADED
	}
	handle := &NodeHandle{
		ID:       ctx.ID,
		Buff:     ctx.Buff,
		Capacity: size,
		State:    state,
		RTState:  nif.NODE_RT_RUNNING,
		Manager:  ctx.Manager,
	}
	return handle
}

func (h *NodeHandle) Iteration() uint64 {
	return h.Iter
}

func (h *NodeHandle) IncIteration() uint64 {
	h.Iter++
	return h.Iter
}

func (h *NodeHandle) FlushData() {
	if h.ID.IsTransient() {
		if !h.Spillable {
			return
		}
		// TODO: flush transient memory
	}
	// TODO: Flush node
}

func (h *NodeHandle) GetBuffer() buf.IBuffer {
	return h.Buff
}

func (h *NodeHandle) Unload() {
	if nif.AtomicLoadState(&h.State) == nif.NODE_UNLOAD {
		return
	}
	if nif.AtomicCASState(&(h.State), nif.NODE_LOADED, nif.NODE_UNLOADING) {
		panic("logic error")
	}
	h.FlushData()
	h.Buff.Close()
	h.Buff = nil
	nif.AtomicStoreState(&(h.State), nif.NODE_UNLOAD)
}

func (h *NodeHandle) GetCapacity() uint64 {
	return h.Capacity
}

func (h *NodeHandle) Ref() {
	atomic.AddUint64(&h.Refs, 1)
}

func (h *NodeHandle) UnRef() bool {
	old := atomic.LoadUint64(&(h.Refs))
	if old == uint64(0) {
		return false
	}
	return atomic.CompareAndSwapUint64(&(h.Refs), old, old-1)
}

func (h *NodeHandle) HasRef() bool {
	v := atomic.LoadUint64(&(h.Refs))
	return v > uint64(0)
}

func (h *NodeHandle) GetID() layout.BlockId {
	return h.ID
}

func (h *NodeHandle) GetState() nif.NodeState {
	return h.State
}

func (h *NodeHandle) Close() error {
	if !nif.AtomicCASRTState(&(h.RTState), nif.NODE_RT_RUNNING, nif.NODE_RT_CLOSED) {
		// Cocurrent senario that other client already call Close before
		return nil
	}
	if h.Buff != nil {
		h.Buff.Close()
	}
	h.Manager.UnregisterNode(h.ID, h.Spillable)
	return nil
}

func (h *NodeHandle) IsClosed() bool {
	state := nif.AtomicLoadRTState(&(h.RTState))
	return state == nif.NODE_RT_CLOSED
}

func (h *NodeHandle) Unloadable() bool {
	if h.State == nif.NODE_UNLOAD {
		return false
	}
	if h.HasRef() {
		return false
	}

	return true
}

func (h *NodeHandle) RollbackLoad() {
	if !nif.AtomicCASState(&(h.State), nif.NODE_LOADING, nif.NODE_ROOLBACK) {
		return
	}
	h.UnRef()
	if h.Buff != nil {
		h.Buff.Close()
	}
	h.Buff = nil
	nif.AtomicStoreState(&(h.State), nif.NODE_UNLOAD)
}

func (h *NodeHandle) PrepareLoad() bool {
	return nif.AtomicCASState(&(h.State), nif.NODE_UNLOAD, nif.NODE_LOADING)
}

func (h *NodeHandle) CommitLoad() error {
	if !nif.AtomicCASState(&(h.State), nif.NODE_LOADING, nif.NODE_COMMIT) {
		return errors.New("logic error")
	}

	// TODO: Load content from io here

	if !nif.AtomicCASState(&(h.State), nif.NODE_COMMIT, nif.NODE_LOADED) {
		return errors.New("logic error")
	}
	return nil
}

func (h *NodeHandle) MakeHandle() nif.IBufferHandle {
	if nif.AtomicLoadState(&(h.State)) != nif.NODE_LOADED {
		panic("Should not call MakeHandle not NODE_LOADED")
	}
	return NewBufferHandle(h, h.Manager)
}

func (h *NodeHandle) SetBuffer(buf buf.IBuffer) error {
	if h.Buff != nil || h.Capacity != uint64(buf.GetCapacity()) {
		return errors.New("logic error")
	}
	h.Buff = buf
	return nil
}

func NewBufferHandle(blk nif.INodeHandle, mgr mgrif.IBufferManager) nif.IBufferHandle {
	h := &BufferHandle{
		Handle:  blk,
		Manager: mgr,
	}
	return h
}

func (h *BufferHandle) GetID() layout.BlockId {
	return h.Handle.GetID()
}

func (h *BufferHandle) Close() error {
	h.Manager.Unpin(h.Handle)
	return nil
}
