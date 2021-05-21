package catalog

import "sync"

type CatalogType uint8

const (
	INVALID CatalogType = iota
	SCHEMA_ENTRY
	TABLE_ENTRY
	INDEX_ENTRY
	SEGMENT_ENTRY
)

type ICatalogEntry interface{}

type CatalogEntry struct {
	Type      CatalogType
	Deleted   bool
	Name      string
	CreatedOn int64
	Catalog   *Catalog
	Set       *CatalogSet
	Child     ICatalogEntry
	Parent    ICatalogEntry
}

type CatalogSet struct {
	sync.RWMutex
	Catalog *Catalog
	Entries map[uint64]ICatalogEntry
}

type Catalog struct {
	sync.RWMutex
	Schemas CatalogSet
}
