package buf

import (
	e "aoe/pkg/engine"
	"aoe/pkg/engine/layout"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	// "os"
)

func TestWriter(t *testing.T) {
	panic_func := func() {
		e.WRITER_FACTORY.MakeWriter(SPILL_WRITER, context.TODO())
	}
	assert.Panics(t, panic_func)

	ctx := context.TODO()
	id := layout.NewTransientID()
	ctx = context.WithValue(ctx, "id", id)
	writer := e.WRITER_FACTORY.MakeWriter(SPILL_WRITER, ctx)
	assert.NotNil(t, writer)
}
