package node

import (
	e "aoe/pkg/engine"
	buf "aoe/pkg/engine/buffer"
	mgrif "aoe/pkg/engine/buffer/manager/iface"
	nif "aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

func NewNodeHandle(ctx *NodeHandleCtx) nif.INodeHandle {
	size := ctx.Size
	state := nif.NODE_UNLOAD
	if ctx.Buff != nil {
		size = ctx.Buff.GetCapacity()
		state = nif.NODE_LOADED
	}
	handle := &NodeHandle{
		ID:        ctx.ID,
		Buff:      ctx.Buff,
		Capacity:  size,
		State:     state,
		RTState:   nif.NODE_RT_RUNNING,
		Manager:   ctx.Manager,
		Spillable: ctx.Spillable,
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

func (h *NodeHandle) FlushData() error {
	if h.ID.IsTransient() {
		if !h.Spillable {
			return nil
		}
		log.Infof("Flushing memory %d", h.GetID().TableID)
		ctx := context.TODO()
		ctx = context.WithValue(ctx, "buffer", h.Buff)
		w := e.WRITER_FACTORY.MakeWriter(NODE_WRITER, ctx)
		return w.Flush()
	}
	// TODO: Flush node
	log.Infof("Flush node %v", h.GetID())
	return nil
}

func (h *NodeHandle) GetBuffer() buf.IBuffer {
	return h.Buff
}

func (h *NodeHandle) Unload() {
	if nif.AtomicLoadState(&h.State) == nif.NODE_UNLOAD {
		return
	}
	if !nif.AtomicCASState(&(h.State), nif.NODE_LOADED, nif.NODE_UNLOADING) {
		panic("logic error")
	}
	err := h.FlushData()
	if err != nil {
		panic(fmt.Sprintf("flush data err: %s", err))
	}
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
	log.Infof("UnregisterNode %v", h.ID)
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

	if h.ID.IsTransient() {
		if !h.Spillable {
			panic("logic error: should not load non-spillable transient memory")
		}
		ctx := context.TODO()
		ctx = context.WithValue(ctx, "buffer", h.Buff)
		r := e.READER_FACTORY.MakeReader(NODE_READER, ctx)
		log.Infof("loading transient memory %d", h.ID.TableID)
		err := r.Load()
		if err != nil {
			return err
		}
	}
	// TODO: Load content

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

func NewBufferHandle(n nif.INodeHandle, mgr mgrif.IBufferManager) nif.IBufferHandle {
	h := &BufferHandle{
		Handle:  n,
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
