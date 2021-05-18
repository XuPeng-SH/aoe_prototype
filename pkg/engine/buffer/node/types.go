package node

import (
	buf "aoe/pkg/engine/buffer"
	mgrif "aoe/pkg/engine/buffer/manager/iface"
	nif "aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	"sync"
)

type NodeBuffer struct {
	buf.IBuffer
	ID layout.BlockId
	// Type nif.BufferType
}

type NodeHandleCtx struct {
	ID          layout.BlockId
	Buff        buf.IBuffer
	Destroyable bool
	Manager     mgrif.IBufferManager
	Size        uint64
}

type NodeHandle struct {
	sync.Mutex
	State       nif.NodeState
	ID          layout.BlockId
	Buff        buf.IBuffer
	Destroyable bool
	Capacity    uint64
	RTState     nif.NodeRTState
	Refs        uint64
	Manager     mgrif.IBufferManager
	Iter        uint64
}

// BufferHandle is created from IBufferManager::Pin, which will set the INodeHandle reference to 1
// The following IBufferManager::Pin will call INodeHandle::Ref to increment the reference count
// BufferHandle should alway be closed manually when it is not needed, which will call IBufferManager::Unpin
type BufferHandle struct {
	Handle  nif.INodeHandle
	Manager mgrif.IBufferManager
}
