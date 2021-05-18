package manager

import (
	nif "aoe/pkg/engine/buffer/node/iface"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

type SimpleEvictHolder struct {
	sync.Mutex
	Queue chan *EvictNode
}

const (
	EVICT_HOLDER_CAPACITY uint64 = 100000
)

func NewSimpleEvictHolder(capacity ...uint64) IEvictHolder {
	c := EVICT_HOLDER_CAPACITY
	if len(capacity) > 0 {
		c = capacity[0]
	}
	holder := &SimpleEvictHolder{
		Queue: make(chan *EvictNode, c),
	}
	return holder
}

func (holder *SimpleEvictHolder) Enqueue(node *EvictNode) {
	log.Infof("Equeue evict h %v", node.Handle.GetID())
	holder.Queue <- node
}

func (holder *SimpleEvictHolder) Dequeue() *EvictNode {
	select {
	case node := <-holder.Queue:
		log.Infof("Dequeue evict h %v", node.Handle.GetID())
		return node
	default:
		log.Info("Dequeue empty evict h")
		return nil
	}
}

func (node *EvictNode) String() string {
	return fmt.Sprintf("EvictNode(%v, %d)", node.Handle, node.Iter)
}

func (node *EvictNode) Unloadable(h nif.INodeHandle) bool {
	if node.Handle != h {
		panic("Logic error")
	}
	return h.Iteration() == node.Iter
}
