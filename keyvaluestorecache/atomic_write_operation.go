package keyvaluestorecache

import "github.com/theaaf/keyvaluestore"

type readCacheAtomicWriteOperation struct {
	ReadCache   *ReadCache
	atomicWrite keyvaluestore.AtomicWriteOperation

	invalidations []string
}

func (op *readCacheAtomicWriteOperation) SetNX(key string, value interface{}) keyvaluestore.AtomicWriteResult {
	op.invalidations = append(op.invalidations, key)
	return op.atomicWrite.SetNX(key, value)
}

func (op *readCacheAtomicWriteOperation) CAS(key string, oldValue, newValue string) keyvaluestore.AtomicWriteResult {
	op.invalidations = append(op.invalidations, key)
	return op.atomicWrite.CAS(key, oldValue, newValue)
}

func (op *readCacheAtomicWriteOperation) Delete(key string) keyvaluestore.AtomicWriteResult {
	op.invalidations = append(op.invalidations, key)
	return op.atomicWrite.Delete(key)
}

func (op *readCacheAtomicWriteOperation) Exec() (bool, error) {
	ret, err := op.atomicWrite.Exec()
	for _, key := range op.invalidations {
		op.ReadCache.cache.Delete(key)
	}
	return ret, err
}
