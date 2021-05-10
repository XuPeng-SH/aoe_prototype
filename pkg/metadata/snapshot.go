package metadata

import (
	"errors"
	log "github.com/sirupsen/logrus"
)

func (handle *BucketCacheHandle) Close() error {
	refs, succ := handle.Unref()
	if !succ {
		return errors.New("logic error closing snapshot")
	}
	if refs == 0 {
		log.Infof("BucketCache %d is ready for releasing.", handle.Cache.Version)
		if handle.OnNoRefFunc != nil {
			handle.OnNoRefFunc(handle)
		}
	}
	return nil
}
