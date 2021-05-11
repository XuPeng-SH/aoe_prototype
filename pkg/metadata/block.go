package metadata

import "fmt"

func NewBlock(id uint64) *Block {
	blk := &Block{
		ID: ID{ID: id},
	}
	return blk
}

func (blk *Block) GetID() ID {
	return blk.ID
}

func (blk *Block) String() string {
	return fmt.Sprintf("Blk(%s)", blk.ID.String())
}
