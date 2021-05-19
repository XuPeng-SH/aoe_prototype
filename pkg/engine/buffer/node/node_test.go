package node

import (
	e "aoe/pkg/engine"
	buf "aoe/pkg/engine/buffer"
	"aoe/pkg/engine/layout"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	// "os"
)

func TestWriter(t *testing.T) {
	panic_func := func() {
		e.WRITER_FACTORY.MakeWriter(NODE_WRITER, context.TODO())
	}
	assert.Panics(t, panic_func)
	node_capacity := uint64(4096)
	capacity := node_capacity * 4
	pool := buf.NewSimpleMemoryPool(capacity)
	assert.NotNil(t, pool)
	node1 := pool.MakeNode(node_capacity)
	assert.NotNil(t, node1)

	id := layout.NewTransientID()
	node_buff1 := NewNodeBuffer(*id, node1)

	ctx := context.TODO()
	ctx = context.WithValue(ctx, "buffer", node_buff1)
	e.WRITER_FACTORY.Dirname = "/tmp/node_test"
	writer := e.WRITER_FACTORY.MakeWriter(NODE_WRITER, ctx)
	assert.NotNil(t, writer)
	err := writer.Flush()
	assert.Nil(t, err)
}
