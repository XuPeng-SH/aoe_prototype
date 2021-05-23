package table

import (
	"aoe/pkg/engine/layout/table/col"
	"sync"
)

type ITableData interface {
	sync.Locker
	GetRowCount() uint64
	GetID() uint64
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

func NewTableData(id uint64, colDefs []IColumnDef) ITableData {
	data := &TableData{
		ID:         id,
		Columns:    make([]col.IColumnData, 0),
		ColumnDefs: colDefs,
	}
	return data
}

type TableData struct {
	sync.Mutex
	ID         uint64
	RowCount   uint64
	Columns    []col.IColumnData
	ColumnDefs []IColumnDef
}

func (td *TableData) GetRowCount() uint64 {
	return td.RowCount
}

func (td *TableData) GetID() uint64 {
	return td.ID
}
