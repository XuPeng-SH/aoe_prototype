package catalog

func NewCatalogSet(cl *Catalog) *CatalogSet {
	set := &CatalogSet{
		Catalog: cl,
		Entries: make(map[uint64]ICatalogEntry),
	}
	return set
}
