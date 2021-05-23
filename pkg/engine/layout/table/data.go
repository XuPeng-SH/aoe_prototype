package table

import (
	bmgrif "aoe/pkg/engine/buffer/manager/iface"
	"aoe/pkg/engine/layout/table/col"
	"sync"
)

type ITableData interface {
	sync.Locker
	GetRowCount() uint64
	GetID() uint64
	GetCollumns() []col.IColumnData
	GetColTypeSize(idx int) uint64
	GetBufMgr() bmgrif.IBufferManager
	// Scan()
}

type IColumnDef interface {
	GetType() interface{}
	TypeSize() uint64
}

type MockColumnDef struct {
}

func (c *MockColumnDef) GetType() interface{} {
	return nil
}

func (c *MockColumnDef) TypeSize() uint64 {
	return uint64(4)
}

func NewTableData(bufMgr bmgrif.IBufferManager, id uint64, colDefs []IColumnDef) ITableData {
	data := &TableData{
		ID:         id,
		Columns:    make([]col.IColumnData, 0),
		ColumnDefs: colDefs,
		BufMgr:     bufMgr,
	}
	return data
}

type TableData struct {
	sync.Mutex
	ID         uint64
	RowCount   uint64
	Columns    []col.IColumnData
	ColumnDefs []IColumnDef
	BufMgr     bmgrif.IBufferManager
}

func (td *TableData) GetRowCount() uint64 {
	return td.RowCount
}

func (td *TableData) GetID() uint64 {
	return td.ID
}

func (td *TableData) GetCollumns() []col.IColumnData {
	return td.Columns
}

func (td *TableData) GetColTypeSize(idx int) uint64 {
	return td.ColumnDefs[idx].TypeSize()
}

func (td *TableData) GetBufMgr() bmgrif.IBufferManager {
	return td.BufMgr
}
