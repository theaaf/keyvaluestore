package keyvaluestorecache

import "github.com/theaaf/keyvaluestore"

type readCacheBatchOperation struct {
	ReadCache *ReadCache

	tryCache       []func()
	getMisses      []boGetMiss
	smembersMisses []boSMembersMiss
	batch          keyvaluestore.BatchOperation
	invalidations  []string
	firstError     error
}

type boGetMiss struct {
	Key    string
	Dest   *boGetResult
	Source keyvaluestore.GetResult
}

type boSMembersMiss struct {
	Key    string
	Dest   *boSMembersResult
	Source keyvaluestore.SMembersResult
}

type boGetResult struct {
	value *string
	err   error
}

func (r *boGetResult) Result() (*string, error) {
	return r.value, r.err
}

func (op *readCacheBatchOperation) Get(key string) keyvaluestore.GetResult {
	result := &boGetResult{}
	op.tryCache = append(op.tryCache, func() {
		v, _ := op.ReadCache.load(key)
		entry, ok := v.(readCacheGetEntry)
		if ok {
			result.value, result.err = entry.value, entry.err
			if result.err != nil && op.firstError == nil {
				op.firstError = result.err
			}
		} else {
			op.getMisses = append(op.getMisses, boGetMiss{
				Key:    key,
				Dest:   result,
				Source: op.batch.Get(key),
			})
		}
	})
	return result
}

func (op *readCacheBatchOperation) Delete(key string) keyvaluestore.DeleteResult {
	op.invalidations = append(op.invalidations, key)
	return op.batch.Delete(key)
}

func (op *readCacheBatchOperation) Set(key string, value interface{}) keyvaluestore.ErrorResult {
	op.invalidations = append(op.invalidations, key)
	return op.batch.Set(key, value)
}

type boSMembersResult struct {
	members []string
	err     error
}

func (r *boSMembersResult) Result() ([]string, error) {
	return r.members, r.err
}

func (op *readCacheBatchOperation) SMembers(key string) keyvaluestore.SMembersResult {
	result := &boSMembersResult{}
	op.tryCache = append(op.tryCache, func() {
		v, _ := op.ReadCache.load(key)
		entry, ok := v.(readCacheSMembersEntry)
		if ok {
			result.members, result.err = entry.members, entry.err
			if result.err != nil && op.firstError == nil {
				op.firstError = result.err
			}
		} else {
			op.smembersMisses = append(op.smembersMisses, boSMembersMiss{
				Key:    key,
				Dest:   result,
				Source: op.batch.SMembers(key),
			})
		}
	})
	return result
}

func (op *readCacheBatchOperation) SAdd(key string, member interface{}, members ...interface{}) keyvaluestore.ErrorResult {
	op.invalidations = append(op.invalidations, key)
	return op.batch.SAdd(key, member, members...)
}

func (op *readCacheBatchOperation) SRem(key string, member interface{}, members ...interface{}) keyvaluestore.ErrorResult {
	op.invalidations = append(op.invalidations, key)
	return op.batch.SRem(key, member, members...)
}

func (op *readCacheBatchOperation) ZAdd(key string, member interface{}, score float64) keyvaluestore.ErrorResult {
	op.invalidations = append(op.invalidations, key)
	return op.batch.ZAdd(key, member, score)
}

func (op *readCacheBatchOperation) ZRem(key string, member interface{}) keyvaluestore.ErrorResult {
	op.invalidations = append(op.invalidations, key)
	return op.batch.ZRem(key, member)
}

func (op *readCacheBatchOperation) Exec() error {
	for _, f := range op.tryCache {
		f()
	}
	if op.firstError != nil || len(op.getMisses)+len(op.smembersMisses)+len(op.invalidations) == 0 {
		return op.firstError
	}
	err := op.batch.Exec()

	for _, miss := range op.getMisses {
		miss.Dest.value, miss.Dest.err = miss.Source.Result()
		op.ReadCache.store(miss.Key, readCacheGetEntry{
			value: miss.Dest.value,
			err:   miss.Dest.err,
		})
	}
	for _, miss := range op.smembersMisses {
		miss.Dest.members, miss.Dest.err = miss.Source.Result()
		op.ReadCache.store(miss.Key, readCacheSMembersEntry{
			members: miss.Dest.members,
			err:     miss.Dest.err,
		})
	}
	for _, key := range op.invalidations {
		op.ReadCache.cache.Delete(key)
	}
	return err
}
