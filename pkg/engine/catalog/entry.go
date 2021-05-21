package catalog

type ISchemaEntry interface {
	GetIndexEntry() interface{}
}

type SchemaEntry struct {
	CatalogEntry
	Tables CatalogSet
	// Indexes CatalogSet
}

func NewSchemaEntry(cl *Catalog, name string) *SchemaEntry {
	entry := &SchemaEntry{
		CatalogEntry: CatalogEntry{
			Type:    SCHEMA_ENTRY,
			Name:    name,
			Catalog: cl,
		},
		Tables: *NewCatalogSet(cl),
		// Indexes: *NewCatalogSet(cl),
	}
	return entry
}

type StandardEntry struct {
	CatalogEntry
	Schema *SchemaEntry
}

func NewStandardEntry(t CatalogType, schema *SchemaEntry, cl *Catalog, name string) *StandardEntry {
	entry := &StandardEntry{
		Schema: schema,
		CatalogEntry: CatalogEntry{
			Type:    t,
			Name:    name,
			Catalog: cl,
		},
	}
	return entry
}

type Index struct{}

type IndexEntry struct {
	StandardEntry
	Index *Index
}

func NewIndexEntry(cl *Catalog, schema *SchemaEntry, name string) *IndexEntry {
	entry := &IndexEntry{
		StandardEntry: *NewStandardEntry(INDEX_ENTRY, schema, cl, name),
	}
	return entry
}

type SegmentEntry struct {
	StandardEntry
}

type DataTable struct{}

type TableEntry struct {
	StandardEntry
	DataTable *DataTable
	NameMap   map[string]uint64
	//Columns []ColumnDef
	Segments CatalogSet
}

func NewTableCatalogEntry(cl *Catalog, schema *SchemaEntry, name string, nameMap map[string]uint64) *TableEntry {
	entry := &TableEntry{
		StandardEntry: *NewStandardEntry(TABLE_ENTRY, schema, cl, name),
		NameMap:       nameMap,
		Segments:      *NewCatalogSet(cl),
	}
	return entry
}
