package dataio

import (
	e "aoe/pkg/engine"
	"aoe/pkg/engine/layout"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
)

type ISegmentFile interface {
	ReadPart(colIdx uint64, id layout.ID, buf []byte)
}

type UnsortedSegmentFile struct {
	sync.RWMutex
	ID     layout.ID
	Blocks map[layout.ID]*BlockFile
}

func NewUnsortedSegmentFile(dirname string, id layout.ID) ISegmentFile {
	usf := &UnsortedSegmentFile{
		ID:     id,
		Blocks: make(map[layout.ID]*BlockFile),
	}
	return usf
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

func NewSortedSegmentFile(dirname string, id layout.ID) ISegmentFile {
	sf := &SortedSegmentFile{
		Parts: make(map[Key]Pointer),
		ID:    id,
	}

	name := e.MakeFilename(dirname, e.FTSegment, id.ToSegmentFileName(), false)
	log.Infof("SegmentFile name %s", name)
	if _, err := os.Stat(name); os.IsNotExist(err) {
		panic(fmt.Sprintf("Specified file %s not existed", name))
	}
	r, err := os.OpenFile(name, os.O_RDONLY, 0666)
	if err != nil {
		panic(fmt.Sprintf("Cannot open specified file %s: %s", name, err))
	}

	sf.File = *r
	sf.initPointers()
	return sf
}

type SortedSegmentFile struct {
	sync.RWMutex
	ID layout.ID
	os.File
	Parts map[Key]Pointer
}

func (sf *SortedSegmentFile) initPointers() {
	// TODO
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
