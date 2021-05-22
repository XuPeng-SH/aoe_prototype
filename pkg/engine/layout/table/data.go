package table

import (
	"aoe/pkg/engine/layout/table/col"
	"sync"
)

type ITableData interface {
	sync.Locker
	GetRowCount() uint64
	// Scan()
}

type TableData struct {
	sync.Mutex
	RowCount uint64
	Columns  []col.IColumnData
}

func (td *TableData) GetRowCount() uint64 {
	return td.RowCount
}
