package col

import (
	nif "aoe/pkg/engine/buffer/node/iface"
)

type IScanCursor interface {
	Next()
}

type ScanCursor struct {
	Current IColumnBlock
	Handle  nif.IBufferHandle
}

func (c *ScanCursor) Next() {
	if c.Current == nil {
		return
	}
	c.Current = c.Current.GetNext()
}
