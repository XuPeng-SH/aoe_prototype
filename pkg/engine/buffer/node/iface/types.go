package iface

import (
	buf "aoe/pkg/engine/buffer"
	"aoe/pkg/engine/layout"
	"io"
	"sync"
	"sync/atomic"
)

type NodeState = uint32

const (
	NODE_UNLOAD NodeState = iota
	NODE_LOADING
	NODE_ROOLBACK
	NODE_COMMIT
	NODE_UNLOADING
	NODE_LOADED
)

func AtomicLoadState(addr *NodeState) NodeState {
	return atomic.LoadUint32(addr)
}

func AtomicStoreState(addr *NodeState, val NodeState) {
	atomic.StoreUint32(addr, val)
}

func AtomicCASState(addr *NodeState, old, new NodeState) bool {
	return atomic.CompareAndSwapUint32(addr, old, new)
}

type NodeRTState = uint32

const (
	NODE_RT_RUNNING NodeRTState = iota
	NODE_RT_CLOSED
)

func AtomicLoadRTState(addr *NodeRTState) NodeRTState {
	return atomic.LoadUint32(addr)
}

func AtomicStoreRTState(addr *NodeRTState, val NodeRTState) {
	atomic.StoreUint32(addr, val)
}

func AtomicCASRTState(addr *NodeRTState, old, new NodeRTState) bool {
	return atomic.CompareAndSwapUint32(addr, old, new)
}

type INodeBuffer interface {
	buf.IBuffer
	GetID() layout.BlockId
}

type INodeHandle interface {
	sync.Locker
	io.Closer
	GetID() layout.BlockId
	Unload()
	// Loadable() bool
	Unloadable() bool
	// GetBuff() buf.IBuffer
	PrepareLoad() bool
	RollbackLoad()
	CommitLoad() error
	MakeHandle() IBufferHandle
	GetState() NodeState
	GetCapacity() uint64
	// Size() uint64
	// IsDestroyable() bool
	IsClosed() bool
	Ref()
	// If the current Refs is already 0, it returns false, else true
	UnRef() bool
	// If the current Refs is not 0, it returns true, else false
	HasRef() bool
	SetBuffer(buffer buf.IBuffer) error
	Iteration() uint64
	IncIteration() uint64
}

type IBufferHandle interface {
	io.Closer
	GetID() layout.BlockId
}
