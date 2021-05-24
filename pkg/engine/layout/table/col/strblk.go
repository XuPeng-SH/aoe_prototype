package col

import (
	"aoe/pkg/engine/layout"
)

type StrColumnBlock struct {
	ColumnBlock
	Parts []IColumnPart
}

func NewStrColumnBlock(seg IColumnSegment, id layout.ID, blkType BlockType) IColumnBlock {
	blk := &StrColumnBlock{
		ColumnBlock: ColumnBlock{
			ID:      id,
			Segment: seg,
			Type:    blkType,
		},
		Parts: make([]IColumnPart, 0),
	}
	seg.Append(blk)
	return blk
}

func (blk *StrColumnBlock) CloneWithUpgrade(seg IColumnSegment) IColumnBlock {
	return nil
}

func (blk *StrColumnBlock) Close() error {
	for _, part := range blk.Parts {
		err := part.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (blk *StrColumnBlock) GetPartRoot() IColumnPart {
	if len(blk.Parts) == 0 {
		return nil
	}
	return blk.Parts[0]
}

func (blk *StrColumnBlock) Append(part IColumnPart) {
	if !blk.ID.IsSameBlock(part.GetID()) {
		panic("logic error")
	}
	if len(blk.Parts) != 0 {
		blk.Parts[len(blk.Parts)-1].SetNext(part)
	}
	blk.Parts = append(blk.Parts, part)
}

func (blk *StrColumnBlock) InitScanCursor(cursor *ScanCursor) error {
	if len(blk.Parts) != 0 {
		return blk.Parts[0].InitScanCursor(cursor)
	}
	return nil
}
