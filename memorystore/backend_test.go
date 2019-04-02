package memorystore

import (
	"testing"

	"github.aaf.cloud/platform/keyvaluestore"
	"github.aaf.cloud/platform/keyvaluestore/keyvaluestoretest"
)

func TestBackend(t *testing.T) {
	keyvaluestoretest.TestBackend(t, func() keyvaluestore.Backend {
		return NewBackend()
	})
}
