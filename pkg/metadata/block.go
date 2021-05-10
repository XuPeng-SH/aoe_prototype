package metadata

func NewBlock(id uint64) *Block {
	blk := &Block{
		ID: ID{ID: id},
	}
	return blk
}

func (blk *Block) GetID() ID {
	return blk.ID
}
