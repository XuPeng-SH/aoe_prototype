package node

import (
	dio "aoe/pkg/engine/dataio"
	ioif "aoe/pkg/engine/dataio/iface"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
)

const (
	NODE_CLEANER = "NC"
)

func init() {
	dio.CLEANER_FACTORY.RegisterBuilder(NODE_CLEANER, &NodeCleanerBuilder{})
}

type NodeCleanerBuilder struct {
}

func (b *NodeCleanerBuilder) Build(rf ioif.ICleanerFactory, ctx context.Context) ioif.Cleaner {
	filename := ""
	fn := ctx.Value("filename")
	if fn != nil {
		filename = fmt.Sprintf("%v", fn)
	}
	r := &NodeCleaner{
		Filename: filename,
	}
	return r
}

type NodeCleaner struct {
	Filename string
}

func (nc *NodeCleaner) Clean() error {
	log.Infof("NodeCleaner removing %s", nc.Filename)
	return os.Remove(nc.Filename)
}
