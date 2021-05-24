package col

import (
	"aoe/pkg/engine/layout"
)

type StdColumnBlock struct {
	ColumnBlock
	Part IColumnPart
}

func NewStdColumnBlock(seg IColumnSegment, id layout.ID, blkType BlockType) IColumnBlock {
	blk := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			ID:      id,
			Segment: seg,
			Type:    blkType,
		},
	}
	seg.Append(blk)
	return blk
}

func (blk *StdColumnBlock) CloneWithUpgrade(seg IColumnSegment) IColumnBlock {
	if blk.Type == PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	var newType BlockType
	if blk.Type == TRANSIENT_BLK {
		newType = PERSISTENT_BLK
	} else {
		newType = PERSISTENT_SORTED_BLK
	}
	cloned := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			ID:      blk.ID,
			Segment: seg,
			Type:    newType,
		},
	}
	return cloned
}

func (blk *StdColumnBlock) GetPartRoot() IColumnPart {
	return blk.Part
}

func (blk *StdColumnBlock) Append(part IColumnPart) {
	if !blk.ID.IsSameBlock(part.GetID()) || blk.Part != nil {
		panic("logic error")
	}
	blk.Part = part
}

func (blk *StdColumnBlock) Close() error {
	if blk.Part != nil {
		return blk.Part.Close()
	}
	return nil
}

func (blk *StdColumnBlock) InitScanCursor(cursor *ScanCursor) error {
	if blk.Part != nil {
		cursor.Current = blk.Part
		return blk.Part.InitScanCursor(cursor)
	}
	return nil
}
