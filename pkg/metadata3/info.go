package md3

import (
	// "encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
	"io"
)

var (
	Meta = *NewMetaInfo()
)

func NewMetaInfo() *MetaInfo {
	info := &MetaInfo{
		Tables: make(map[uint64]*Table),
	}
	return info
}

func (info *MetaInfo) ReferenceTable(table_id uint64) (tbl *Table, err error) {
	info.RLock()
	defer info.RUnlock()
	tbl, ok := info.Tables[table_id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("specified table %d not found in info", table_id))
	}
	return tbl, nil
}

func (info *MetaInfo) ReferenceBlock(table_id, segment_id, block_id uint64) (blk *Block, err error) {
	info.RLock()
	tbl, ok := info.Tables[table_id]
	if !ok {
		info.RUnlock()
		return nil, errors.New(fmt.Sprintf("specified table %d not found in info", table_id))
	}
	info.RUnlock()
	blk, err = tbl.ReferenceBlock(segment_id, block_id)

	return blk, err
}

func (info *MetaInfo) TableIDs(args ...int64) map[uint64]uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0]
	}
	ids := make(map[uint64]uint64)
	info.RLock()
	defer info.RUnlock()
	for _, t := range info.Tables {
		if !t.Select(ts) {
			continue
		}
		ids[t.GetID()] = t.GetID()
	}
	return ids
}

func (info *MetaInfo) CreateTable() (tbl *Table, err error) {
	tbl = NewTable(Meta.Sequence.GetTableID())
	return tbl, err
}

func (info *MetaInfo) String() string {
	s := fmt.Sprintf("Info(ck=%d)", info.CheckPoint)
	s += "["
	for i, t := range info.Tables {
		if i != 0 {
			s += "\n"
		}
		s += t.String()
	}
	if len(info.Tables) > 0 {
		s += "\n"
	}
	s += "]"
	return s
}

func (info *MetaInfo) RegisterTable(tbl *Table) error {
	info.Lock()
	defer info.Unlock()

	_, ok := info.Tables[tbl.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate table %d found in info", tbl.ID))
	}
	err := tbl.Attach()
	if err != nil {
		return err
	}

	info.Tables[tbl.ID] = tbl
	return nil
}

func (info *MetaInfo) Copy(ts ...int64) *MetaInfo {
	var t int64
	if len(ts) == 0 {
		t = NowMicro()
	} else {
		t = ts[0]
	}
	new_info := NewMetaInfo()
	for k, v := range info.Tables {
		if !v.Select(t) {
			continue
		}
		tbl := v.Copy(ts...)
		new_info.Tables[k] = tbl
	}

	return new_info
}

func (info *MetaInfo) Serialize(w io.Writer) error {
	return msgpack.NewEncoder(w).Encode(info)
}

func Deserialize(r io.Reader) (info *MetaInfo, err error) {
	log.Info("")
	info = NewMetaInfo()
	err = msgpack.NewDecoder(r).Decode(info)
	return info, err
}
