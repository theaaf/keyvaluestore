package memorystore

import (
	"testing"

	"github.com/theaaf/keyvaluestore"
	"github.com/theaaf/keyvaluestore/keyvaluestoretest"
)

func TestBackend(t *testing.T) {
	keyvaluestoretest.TestBackend(t, func() keyvaluestore.Backend {
		return NewBackend()
	})
}
