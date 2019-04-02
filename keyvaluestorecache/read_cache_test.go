package keyvaluestorecache_test

import (
	"testing"

	"github.aaf.cloud/platform/keyvaluestore"
	"github.aaf.cloud/platform/keyvaluestore/keyvaluestorecache"
	"github.aaf.cloud/platform/keyvaluestore/keyvaluestoretest"
	"github.aaf.cloud/platform/keyvaluestore/memorystore"
)

func TestReadCache(t *testing.T) {
	keyvaluestoretest.TestBackend(t, func() keyvaluestore.Backend {
		return keyvaluestorecache.NewReadCache(memorystore.NewBackend())
	})
}
