package engine

import (
	"fmt"
	// "os"
	"path"
)

type FileType int

const (
	FTCheckpoint FileType = iota
	FTLock
	FTBlock
	FTSegment
	FTSegmentIndex
)

func MakeFilename(dirname string, ft FileType, id uint64) string {
	switch ft {
	case FTCheckpoint:
		return path.Join(dirname, fmt.Sprintf("%d.ckp", id))
	}
	panic(fmt.Sprintf("unsupported %d", ft))
}
