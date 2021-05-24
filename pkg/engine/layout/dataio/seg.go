package dataio

import (
	"aoe/pkg/engine/layout"
	"fmt"
	"os"
	"sync"
)

type ISegmentFile interface {
	ReadPart(colIdx uint64, id layout.ID, buf []byte)
}

type UnsortedSegmentFile struct {
	sync.RWMutex
	Blocks map[layout.ID]*BlockFile
}

func (sf *UnsortedSegmentFile) AddBlock(id layout.ID, bf *BlockFile) {
	_, ok := sf.Blocks[id]
	if ok {
		panic("logic error")
	}
	sf.Blocks[id] = bf
}

func (sf *UnsortedSegmentFile) ReadPart(colIdx uint64, id layout.ID, buf []byte) {
	blk, ok := sf.Blocks[id.AsBlockID()]
	if !ok {
		panic("logic error")
	}
	blk.ReadPart(colIdx, id, buf)
}

type Pointer struct {
	Offset int64
	Len    uint64
}

type Key struct {
	Col uint64
	ID  layout.ID
}

type SortedSegmentFile struct {
	sync.RWMutex
	os.File
	Parts map[Key]Pointer
}

func (sf *SortedSegmentFile) ReadPart(colIdx uint64, id layout.ID, buf []byte) {
	key := Key{
		Col: colIdx,
		ID:  id,
	}
	pointer, ok := sf.Parts[key]
	if !ok {
		panic("logic error")
	}
	if len(buf) != int(pointer.Len) {
		panic("logic error")
	}
	n, err := sf.ReadAt(buf, pointer.Offset)
	if err != nil {
		panic(fmt.Sprintf("logic error: %s", err))
	}
	if n != int(pointer.Len) {
		panic("logic error")
	}
}
