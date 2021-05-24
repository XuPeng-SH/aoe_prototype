package dataio

import (
	"aoe/pkg/engine/layout"
	"sync"
)

type Manager struct {
	sync.RWMutex
	Files map[layout.ID]ISegmentFile
}

func (mgr *Manager) RegisterSegment(id layout.ID, sf ISegmentFile) {
	mgr.Files[id] = sf
}

func (mgr *Manager) DropSegment(id layout.ID) {
	delete(mgr.Files, id)
}

func (mgr *Manager) GetFile(id layout.ID) ISegmentFile {
	f, ok := mgr.Files[id]
	if !ok {
		panic("logic error")
	}
	return f
}
