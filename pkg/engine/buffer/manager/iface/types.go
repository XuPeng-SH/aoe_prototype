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

	String() string
	NodeCount() int

	RegisterMemory(capacity uint64, spillable bool) nif.INodeHandle
	RegisterSpillableNode(capacity uint64, node_id layout.ID) nif.INodeHandle
	RegisterNode(capacity uint64, node_id layout.ID, segFile interface{}) nif.INodeHandle
	UnregisterNode(nif.INodeHandle)

	// // Allocate(size uint64) buf.IBufferH

	Pin(h nif.INodeHandle) nif.IBufferHandle
	Unpin(h nif.INodeHandle)
}
