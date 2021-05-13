package ops

import (
	md "aoe/pkg/metadata3"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
	// log "github.com/sirupsen/logrus"
)

func TestBasicOps(t *testing.T) {
	worker := NewOperationWorker()
	worker.Start()

	now := time.Now()

	opCtx := OperationContext{}
	op := NewCreateTableOperation(&opCtx, &md.Meta, worker)
	op.Push()
	err := op.WaitDone()
	assert.Nil(t, err)

	tbl := op.GetTable()
	assert.NotNil(t, tbl)

	t.Log(md.Meta.String())
	opCtx = OperationContext{TableID: tbl.ID}
	blkop := NewCreateBlockOperation(&opCtx, &md.Meta, worker)
	blkop.Push()
	err = blkop.WaitDone()
	assert.Nil(t, err)

	blk1 := blkop.GetBlock()
	assert.NotNil(t, blk1)
	assert.Equal(t, blk1.GetBoundState(), md.Detatched)

	assert.Equal(t, blk1.DataState, md.EMPTY)
	blk1.SetCount(blk1.MaxRowCount)
	assert.Equal(t, blk1.DataState, md.FULL)

	blk2, err := md.Meta.ReferenceBlock(blk1.TableID, blk1.SegmentID, blk1.ID)
	assert.Nil(t, err)
	assert.Equal(t, blk2.DataState, md.EMPTY)
	assert.Equal(t, blk2.Count, uint64(0))

	opCtx = OperationContext{Block: blk1}
	updateop := NewUpdateOperation(&opCtx, &md.Meta, worker)
	updateop.Push()
	err = updateop.WaitDone()
	assert.Nil(t, err)

	blk3, err := md.Meta.ReferenceBlock(blk1.TableID, blk1.SegmentID, blk1.ID)
	assert.Nil(t, err)
	assert.Equal(t, blk3.DataState, md.FULL)
	assert.Equal(t, blk1.Count, blk3.Count)

	for i := 0; i < 100; i++ {
		opCtx = OperationContext{TableID: blk1.TableID}
		blkop = NewCreateBlockOperation(&opCtx, &md.Meta, worker)
		blkop.Push()
		err = blkop.WaitDone()
		assert.Nil(t, err)
	}
	du := time.Since(now)
	t.Log(du)

	info_copy := md.Meta.Copy()

	path := "/tmp/tttttt"
	w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	assert.Equal(t, err, nil)
	err = info_copy.Serialize(w)
	assert.Nil(t, err)
	w.Close()

	r, err := os.OpenFile(path, os.O_RDONLY, 0666)
	assert.Equal(t, err, nil)

	de_info, err := md.Deserialize(r)
	assert.Nil(t, err)
	assert.NotNil(t, de_info)
	assert.Equal(t, md.Meta.Sequence.NextBlockID, de_info.Sequence.NextBlockID)
	assert.Equal(t, md.Meta.Sequence.NextSegmentID, de_info.Sequence.NextSegmentID)
	assert.Equal(t, md.Meta.Sequence.NextTableID, de_info.Sequence.NextTableID)

	r.Close()

	du = time.Since(now)
	t.Log(du)

	worker.Stop()
}
