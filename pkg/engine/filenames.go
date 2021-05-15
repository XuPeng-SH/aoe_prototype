package engine

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

type FileType int

const (
	FTCheckpoint FileType = iota
	FTLock
	FTBlock
	FTSegment
	FTSegmentIndex
)

func MakeFilename(dirname string, ft FileType, id uint64, isTmp bool) string {
	var s string
	switch ft {
	case FTCheckpoint:
		s = path.Join(dirname, fmt.Sprintf("%d.ckp", id))
	default:
		panic(fmt.Sprintf("unsupported %d", ft))
	}
	if isTmp {
		s += ".tmp"
	}
	return s
}

func FilenameFromTmpfile(tmpFile string) (fname string, err error) {
	fname = strings.TrimSuffix(tmpFile, ".tmp")
	if len(fname) == len(tmpFile) {
		return "", errors.New(fmt.Sprintf("Cannot extract filename from temp file %s", tmpFile))
	}
	return fname, nil
}
