package metadata

import (
	"sync/atomic"
)

func (proxy *RefProxy) Ref() bool {
	atomic.AddUint64(&(proxy.Refs), 1)
	return true
}

func (proxy *RefProxy) Unref() (uint64, bool) {
	curr := atomic.LoadUint64(&(proxy.Refs))
	if curr == 0 {
		return 0, false
	}
	new_val := curr - 1
	for {
		succ := atomic.CompareAndSwapUint64(&(proxy.Refs), curr, new_val)
		if succ {
			return new_val, true
		}
		curr = atomic.LoadUint64(&(proxy.Refs))
		if curr == 0 {
			return 0, false
		}
		new_val = curr - 1
	}
}

func (proxy *RefProxy) HasRef() bool {
	val := atomic.LoadUint64(&(proxy.Refs))
	return val > 0
}
