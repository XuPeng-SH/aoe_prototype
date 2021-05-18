package iface

import (
	buf "aoe/pkg/engine/buffer"
	nif "aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	"sync"
)

type IBufferManager interface {
	sync.Locker

	GetUsage() uint64
	GetCapacity() uint64
	SetCapacity(c uint64) error

	RegisterNode(node_id layout.BlockId) nif.INodeHandle
	UnregisterNode(node_id layout.BlockId, can_destroy bool)

	// RegisterMemory(node_id layout.BlockId, can_destroy bool) blk.INodeHandle
	// // Allocate(size uint64) buf.IBufferH

	Pin(h nif.INodeHandle) nif.IBufferHandle
	Unpin(h nif.INodeHandle)

	GetPool() buf.IMemoryPool
}
