package ops

import (
	md "aoe/pkg/metadata"
	// "github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateSegmentOp(t *testing.T) {
	ss := md.CacheHolder.GetSnapshot()
	t.Log(ss.Cache.String())

	opCtx := OperationContext{}
	op := NewCreateSegmentOperation(&opCtx, ss)
	seg, err := op.CommitNewSegment()
	t.Log(err)
	t.Log(seg.String())
	t.Log(ss.Cache.String())

	op.OnExecute()
	t.Log(ss.Cache.String())
	new_ss := md.CacheHolder.GetSnapshot()
	t.Log(new_ss.Cache.String())
}
