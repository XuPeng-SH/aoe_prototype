package col

import (
	nif "aoe/pkg/engine/buffer/node/iface"
	"errors"
	"io"
)

type IScanCursor interface {
	io.Closer
	Next()
	Init() error
	IsInited() bool
}

type ScanCursor struct {
	Current IColumnBlock
	Handle  nif.IBufferHandle
	Inited  bool
}

func (c *ScanCursor) Next() {
	if c.Current == nil {
		return
	}
	c.Close()
	c.Current = c.Current.GetNext()
}

func (c *ScanCursor) IsInited() bool {
	return c.Inited
}

func (c *ScanCursor) Init() error {
	if c.Inited {
		return errors.New("Cannot init already init'ed cursor")
	}
	if c.Current == nil {
		return errors.New("Cannot init due to no block")
	}
	err := c.Current.InitScanCursor(c)
	if err != nil {
		return err
	}
	c.Inited = true
	return err
}

func (c *ScanCursor) Close() error {
	c.Inited = false
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
