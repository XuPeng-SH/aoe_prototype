package col

// import (
// 	"aoe/pkg/engine/layout"
// )

// type StrColumnBlock struct {
// 	ColumnBlock
// 	Parts []IColumnPart
// }

// func NewStrColumnBlock(seg IColumnSegment, id layout.BlockId) IColumnBlock {
// 	blk := &StrColumnBlock{
// 		ColumnBlock: ColumnBlock{
// 			ID:      id,
// 			Segment: seg,
// 		},
// 	}
// 	seg.Append(blk)
// 	return blk
// }

// func (blk *StrColumnBlock) GetPartRoot() IColumnPart {
// 	if len(blk.Parts) == 0 {
// 		return nil
// 	}
// 	return blk.Parts[0]
// }

// func (blk *StrColumnBlock) Append(part IColumnPart) {
// 	if !blk.ID.IsSameBlock(part.GetID()) {
// 		panic("logic error")
// 	}
// 	blk.Parts = append(blk.Parts, part)
// }

// func (blk *StrColumnBlock) InitScanCursor(cursor *ScanCursor) error {
// 	if len(blk.Parts) != 0 {
// 		return blk.Parts[0].InitScanCursor(cursor)
// 	}
// 	return nil
// }
