package manager

import (
	buf "aoe/pkg/engine/buffer"
	"aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	"sync"
)

type EvictNode struct {
	Handle iface.INodeHandle
	Iter   uint64
}

type IEvictHolder interface {
	sync.Locker
	Enqueue(n *EvictNode)
	Dequeue() *EvictNode
}

type BufferManager struct {
	buf.IMemoryPool
	sync.Mutex
	Nodes map[layout.BlockId]iface.INodeHandle // Manager is not responsible to Close handle
	// TransientID layout.BlockId
	EvictHolder IEvictHolder
}
