package node

import (
	// e "aoe/pkg/engine"
	buf "aoe/pkg/engine/buffer"
	dio "aoe/pkg/engine/dataio"
	"aoe/pkg/engine/layout"
	"github.com/stretchr/testify/assert"
	"testing"
	// "os"
	// "context"
)

var WORK_DIR = "/tmp/buff/node_test"

func init() {
	dio.WRITER_FACTORY.Init(nil, WORK_DIR)
	dio.READER_FACTORY.Init(nil, WORK_DIR)
}

// func TestWriter(t *testing.T) {
// 	panic_func := func() {
// 		e.WRITER_FACTORY.MakeWriter(NODE_WRITER, context.TODO())
// 	}
// 	assert.Panics(t, panic_func)
// 	node_capacity := uint64(4096)
// 	capacity := node_capacity * 4
// 	pool := buf.NewSimpleMemoryPool(capacity)
// 	assert.NotNil(t, pool)
// 	node1 := pool.MakeNode(node_capacity)
// 	assert.NotNil(t, node1)

// 	id := layout.NewTransientID()
// 	node_buff1 := NewNodeBuffer(*id, node1)

// 	ctx := context.TODO()
// 	ctx = context.WithValue(ctx, "buffer", node_buff1)
// 	os.RemoveAll(e.WRITER_FACTORY.Dirname)
// 	writer := e.WRITER_FACTORY.MakeWriter(NODE_WRITER, ctx)
// 	assert.NotNil(t, writer)
// 	err := writer.Flush()
// 	assert.Nil(t, err)
// 	_, err = os.Stat((writer.(*NodeWriter)).Filename)
// 	assert.Nil(t, err)
// }

func TestNode(t *testing.T) {
	node_capacity := uint64(4096)
	capacity := node_capacity * 4
	pool := buf.NewSimpleMemoryPool(capacity)
	assert.NotNil(t, pool)
	assert.Equal(t, capacity, pool.GetCapacity())
	assert.Equal(t, uint64(0), pool.GetUsage())
	node1 := pool.MakeNode(node_capacity)
	assert.NotNil(t, node1)
	assert.Equal(t, node_capacity, node1.Capacity)
	assert.Equal(t, capacity, pool.GetCapacity())
	assert.Equal(t, node_capacity, pool.GetUsage())

	id := layout.NewTransientID()
	id1 := id.Next()
	node_buff1 := NewNodeBuffer(*id1, node1)
	assert.Equal(t, node_buff1.GetCapacity(), node_capacity)
	assert.Equal(t, *id1, node_buff1.GetID())

	node2 := pool.MakeNode(node_capacity)
	assert.NotNil(t, node2)
	id2 := id.Next()
	node_buff2 := NewNodeBuffer(*id2, node2)
	assert.Equal(t, node_buff2.GetCapacity(), node_capacity)
	assert.Equal(t, *id2, node_buff2.GetID())
	assert.Equal(t, 2*node_capacity, pool.GetUsage())

	node_buff1.Close()
	assert.Equal(t, node_buff1.GetCapacity(), uint64(0))
	assert.Equal(t, node_capacity, pool.GetUsage())

	node_buff2.Close()
	assert.Equal(t, node_buff2.GetCapacity(), uint64(0))
	assert.Equal(t, uint64(0), pool.GetUsage())
}

func TestHandle(t *testing.T) {
	id := layout.NewTransientID()
	ctx := NodeHandleCtx{
		ID: *id,
	}

	handle := NewNodeHandle(&ctx)
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(0))
	assert.False(t, handle.HasRef())

	handle.Ref()
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(1))
	assert.True(t, handle.HasRef())

	handle.Ref()
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(2))
	assert.True(t, handle.HasRef())

	handle.UnRef()
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(1))
	assert.True(t, handle.HasRef())

	handle.UnRef()
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(0))
	assert.False(t, handle.HasRef())

	handle.UnRef()
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(0))
	assert.False(t, handle.HasRef())
}
