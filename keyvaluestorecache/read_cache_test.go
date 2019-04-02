package keyvaluestorecache_test

import (
	"testing"

	"github.com/theaaf/keyvaluestore"
	"github.com/theaaf/keyvaluestore/keyvaluestorecache"
	"github.com/theaaf/keyvaluestore/keyvaluestoretest"
	"github.com/theaaf/keyvaluestore/memorystore"
)

func TestReadCache(t *testing.T) {
	keyvaluestoretest.TestBackend(t, func() keyvaluestore.Backend {
		return keyvaluestorecache.NewReadCache(memorystore.NewBackend())
	})
}
