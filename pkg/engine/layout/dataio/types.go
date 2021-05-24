package dataio

import (
	"aoe/pkg/engine/layout"
)

type Pointer struct {
	Offset int64
	Len    uint64
}

type Key struct {
	Col uint64
	ID  layout.ID
}
