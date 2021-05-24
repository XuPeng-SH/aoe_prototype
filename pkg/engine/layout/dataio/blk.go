package dataio

import (
	"aoe/pkg/engine/layout"
	"fmt"
	"os"
	"sync"
)

type BlockFile struct {
	sync.RWMutex
	os.File
	Parts map[Key]Pointer
}

func (bf *BlockFile) ReadPart(colIdx uint64, id layout.ID, buf []byte) {
	key := Key{
		Col: colIdx,
		ID:  id,
	}
	pointer, ok := bf.Parts[key]
	if !ok {
		panic("logic error")
	}
	if len(buf) != int(pointer.Len) {
		panic("logic error")
	}
	n, err := bf.ReadAt(buf, pointer.Offset)
	if err != nil {
		panic(fmt.Sprintf("logic error: %s", err))
	}
	if n != int(pointer.Len) {
		panic("logic error")
	}
}
