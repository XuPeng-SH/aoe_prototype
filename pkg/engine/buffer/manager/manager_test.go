package manager

import (
	// e "aoe/pkg/engine"
	nif "aoe/pkg/engine/buffer/node/iface"
	dio "aoe/pkg/engine/dataio"
	"aoe/pkg/engine/layout"
	ldio "aoe/pkg/engine/layout/dataio"
	w "aoe/pkg/engine/worker"
	"github.com/stretchr/testify/assert"
	"testing"
)

var WORK_DIR = "/tmp/buff/manager_test"

func init() {
	dio.WRITER_FACTORY.Init(nil, WORK_DIR)
	dio.READER_FACTORY.Init(nil, WORK_DIR)
}

func TestManagerBasic(t *testing.T) {
	flusher := w.NewOpWorker()
	mgr := NewBufferManager(uint64(1), flusher)
	baseid := layout.NewTransientID()
	baseid.TableID = uint64(0)
	node0 := baseid.Next()
	node1 := baseid.Next()
	node2 := baseid.Next()

	node_capacity := uint64(64)

	assert.Equal(t, len(mgr.(*BufferManager).Nodes), 0)
	empty := &ldio.ColSegmentFile{}
	h0 := mgr.RegisterNode(node_capacity, *node0, empty)
	assert.NotNil(t, h0)
	assert.Equal(t, len(mgr.(*BufferManager).Nodes), 1)
	assert.Equal(t, *node0, h0.GetID())

	h1 := mgr.RegisterNode(node_capacity, *node1, empty)
	assert.NotNil(t, h1)
	assert.Equal(t, len(mgr.(*BufferManager).Nodes), 2)
	assert.Equal(t, *node1, h1.GetID())

	h0_1 := mgr.RegisterNode(node_capacity, *node0, empty)
	assert.NotNil(t, h0_1)
	assert.Equal(t, len(mgr.(*BufferManager).Nodes), 2)
	assert.Equal(t, *node0, h0_1.GetID())

	h2 := mgr.RegisterNode(node_capacity, *node2, empty)
	assert.NotNil(t, h2)
	assert.Equal(t, len(mgr.(*BufferManager).Nodes), 3)
	assert.Equal(t, *node2, h2.GetID())

	h1.Close()
	assert.True(t, h1.IsClosed())
	mgr_h1, ok := mgr.(*BufferManager).Nodes[*node1]
	assert.False(t, ok)
	assert.Equal(t, mgr_h1, nil)
	mgr_h2, ok := mgr.(*BufferManager).Nodes[*node2]
	assert.True(t, ok)
	assert.False(t, mgr_h2.IsClosed())

	assert.Equal(t, len(mgr.(*BufferManager).Nodes), 2)
	mgr.UnregisterNode(h0)
	assert.Equal(t, len(mgr.(*BufferManager).Nodes), 1)
}

func TestManager2(t *testing.T) {
	flusher := w.NewOpWorker()
	capacity := uint64(1024)
	node_capacity := 2 * capacity
	mgr := NewBufferManager(capacity, flusher)
	node0 := layout.ID{}
	empty := &ldio.MockColSegmentFile{}
	h0 := mgr.RegisterNode(node_capacity, node0, empty)
	assert.Equal(t, h0.GetID(), node0)
	assert.False(t, h0.HasRef())
	b0 := mgr.Pin(h0)
	assert.Nil(t, b0)
	new_cap := h0.GetCapacity() * 2
	mgr.SetCapacity(new_cap)
	assert.Equal(t, mgr.GetCapacity(), new_cap)

	assert.Equal(t, len(mgr.(*BufferManager).Nodes), 1)
	assert.False(t, h0.HasRef())

	b0 = mgr.Pin(h0)
	assert.Equal(t, b0.GetID(), node0)
	assert.True(t, h0.HasRef())
	b0.Close()
	assert.False(t, h0.HasRef())
	b0 = mgr.Pin(h0)
	assert.True(t, h0.HasRef())
	b1 := mgr.Pin(h0)
	assert.True(t, h0.HasRef())
	b2 := mgr.Pin(h0)
	assert.True(t, h0.HasRef())
	b0.Close()
	assert.True(t, h0.HasRef())
	b2.Close()
	assert.True(t, h0.HasRef())
	b1.Close()
	assert.False(t, h0.HasRef())
}

func TestManager3(t *testing.T) {
	node_capacity := uint64(1024)
	capacity := node_capacity * 2
	flusher := w.NewOpWorker()
	mgr := NewBufferManager(capacity, flusher)
	assert.Equal(t, mgr.GetCapacity(), capacity)

	id := layout.ID{}
	n0 := *id.Next()
	empty := &ldio.MockColSegmentFile{}
	h0 := mgr.RegisterNode(node_capacity, n0, empty)
	assert.NotNil(t, h0)
	assert.Equal(t, h0.GetID(), n0)
	assert.Equal(t, h0.GetState(), nif.NODE_UNLOAD)
	assert.Equal(t, mgr.GetCapacity(), capacity)

	{
		bh0 := mgr.Pin(h0)
		assert.Equal(t, bh0.GetID(), n0)
		assert.Equal(t, h0.GetState(), nif.NODE_LOADED)
		assert.Equal(t, mgr.GetUsage(), h0.GetCapacity())
		assert.Equal(t, mgr.GetCapacity(), capacity)
		assert.True(t, h0.HasRef())
		bh0.Close()
		assert.False(t, h0.HasRef())

		n1 := *id.Next()
		h1 := mgr.RegisterNode(node_capacity, n1, empty)
		assert.True(t, h1 != nil)
		assert.Equal(t, h1.GetID(), n1)
		assert.Equal(t, h1.GetState(), nif.NODE_UNLOAD)
		assert.Equal(t, mgr.GetUsage(), h0.GetCapacity())
		bh_0_1 := mgr.Pin(h1)
		assert.Equal(t, mgr.GetUsage(), h0.GetCapacity()+h1.GetCapacity())
		assert.Equal(t, h1.GetState(), nif.NODE_LOADED)
		bh_0_1.Close()
		assert.Equal(t, mgr.GetUsage(), h0.GetCapacity()+h1.GetCapacity())

		n2 := *id.Next()
		h2 := mgr.RegisterNode(node_capacity, n2, empty)
		assert.True(t, h2 != nil)
		assert.Equal(t, h2.GetID(), n2)
		assert.Equal(t, h2.GetState(), nif.NODE_UNLOAD)
		assert.Equal(t, mgr.GetUsage(), h0.GetCapacity()+h1.GetCapacity())
		bh_0_2 := mgr.Pin(h2)
		assert.NotNil(t, bh_0_2)
		assert.Equal(t, mgr.GetUsage(), h0.GetCapacity()+h1.GetCapacity())
		assert.Equal(t, h1.GetState(), nif.NODE_LOADED)
		bh_0_2.Close()
		// t.Log(bh_0_2)
		// assert.Equal(t, mgr.GetUsage(), h0.GetCapacity()+h1.GetCapacity())
	}
}

func TestManager4(t *testing.T) {
	node_capacity := uint64(1024)
	capacity := node_capacity / 2
	flusher := w.NewOpWorker()
	mgr := NewBufferManager(capacity, flusher)
	assert.Equal(t, mgr.GetCapacity(), capacity)

	baseid := layout.ID{}
	id0 := *baseid.Next()
	h0 := mgr.RegisterSpillableNode(node_capacity, id0)
	assert.Nil(t, h0)
	num_nodes := uint64(3)
	mgr.SetCapacity(num_nodes * node_capacity)

	h0_1 := mgr.RegisterSpillableNode(node_capacity, id0)
	assert.NotNil(t, h0_1)
	assert.Equal(t, h0_1.GetCapacity(), node_capacity)
	assert.Equal(t, h0_1.GetCapacity(), mgr.GetUsage())
	assert.Equal(t, nif.NODE_LOADED, h0_1.GetState())
	assert.False(t, h0_1.HasRef())

	bh0 := mgr.Pin(h0_1)
	assert.Equal(t, bh0.GetID(), id0)
	assert.Equal(t, nif.NODE_LOADED, h0_1.GetState())
	assert.Equal(t, h0_1.GetCapacity(), node_capacity)
	assert.Equal(t, h0_1.GetCapacity(), mgr.GetUsage())
	assert.True(t, h0_1.HasRef())
	bh0.Close()
	assert.False(t, h0_1.HasRef())
}

func TestManager5(t *testing.T) {
	node_capacity := uint64(1024)
	capacity := node_capacity / 2
	flusher := w.NewOpWorker()
	mgr := NewBufferManager(capacity, flusher)
	assert.Equal(t, mgr.GetCapacity(), capacity)

	h0 := mgr.RegisterMemory(node_capacity, false)
	assert.Nil(t, h0)

	mgr.SetCapacity(node_capacity)

	h0_1 := mgr.RegisterMemory(node_capacity, true)
	assert.NotNil(t, h0_1)
	assert.Equal(t, h0_1.GetCapacity(), node_capacity)
	assert.Equal(t, h0_1.GetCapacity(), mgr.GetUsage())
	assert.Equal(t, nif.NODE_LOADED, h0_1.GetState())
	assert.False(t, h0_1.HasRef())

	bh0 := mgr.Pin(h0_1)
	bh0_id := bh0.GetID()
	assert.True(t, bh0_id.IsTransient())
	assert.Equal(t, nif.NODE_LOADED, h0_1.GetState())
	assert.Equal(t, h0_1.GetCapacity(), node_capacity)
	assert.Equal(t, h0_1.GetCapacity(), mgr.GetUsage())
	assert.True(t, h0_1.HasRef())

	h1 := mgr.RegisterMemory(node_capacity, false)
	assert.Nil(t, h1)

	bh0.Close()
	assert.False(t, h0_1.HasRef())
	id := layout.ID{}
	id1 := id.Next()
	h1 = mgr.RegisterSpillableNode(node_capacity, *id1)
	assert.NotNil(t, h1)

	bh1 := mgr.Pin(h1)
	assert.Equal(t, nif.NODE_LOADED, h1.GetState())
	assert.Equal(t, h1.GetCapacity(), node_capacity)
	assert.Equal(t, h1.GetCapacity(), mgr.GetUsage())
	assert.True(t, h1.HasRef())

	bh1.Close()
	assert.False(t, h1.HasRef())

	bh0 = mgr.Pin(h0_1)
	assert.NotNil(t, bh0)
	bh1 = mgr.Pin(h1)
	assert.Nil(t, bh1)

	bh0.Close()
	bh1 = mgr.Pin(h1)
	assert.NotNil(t, bh1)

	h1.Close()
	h0_1.Close()
}
