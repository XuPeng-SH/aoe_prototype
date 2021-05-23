package manager

import (
	buf "aoe/pkg/engine/buffer"
	"aoe/pkg/engine/buffer/node/iface"
	"aoe/pkg/engine/layout"
	iw "aoe/pkg/engine/worker/base"
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
	sync.RWMutex
	Nodes       map[layout.ID]iface.INodeHandle // Manager is not responsible to Close handle
	TransientID layout.ID
	EvictHolder IEvictHolder
	Flusher     iw.IOpWorker
}
