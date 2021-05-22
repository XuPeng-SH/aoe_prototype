package col

import (
	nif "aoe/pkg/engine/buffer/node/iface"
	"io"
)

type IScanCursor interface {
	io.Closer
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

func (c *ScanCursor) Close() error {
	if c.Handle != nil {
		err := c.Handle.Close()
		if err != nil {
			panic("logic error")
		}
		c.Handle = nil
		return nil
	}
	return nil
}
