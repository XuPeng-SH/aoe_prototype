package iface

import (
	buf "aoe/pkg/engine/buffer"
	nif "aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	"sync"
)

type IBufferManager interface {
	sync.Locker
	RLock()
	RUnlock()
	buf.IMemoryPool

	RegisterMemory(capacity uint64, spillable bool) nif.INodeHandle
	RegisterTransientNode(capacity uint64, node_id layout.BlockId) nif.INodeHandle
	RegisterNode(capacity uint64, node_id layout.BlockId) nif.INodeHandle
	UnregisterNode(node_id layout.BlockId, spillable bool)

	// // Allocate(size uint64) buf.IBufferH

	Pin(h nif.INodeHandle) nif.IBufferHandle
	Unpin(h nif.INodeHandle)
}