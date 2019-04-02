package keyvaluestore

type DeleteResult interface {
	Result() (bool, error)
}

type GetResult interface {
	Result() (*string, error)
}

type SMembersResult interface {
	Result() ([]string, error)
}

type ErrorResult interface {
	Result() error
}

type BatchOperation interface {
	Get(key string) GetResult
	Delete(key string) DeleteResult
	Set(key string, value interface{}) ErrorResult
	SMembers(key string) SMembersResult
	SAdd(key string, member interface{}, members ...interface{}) ErrorResult
	SRem(key string, member interface{}, members ...interface{}) ErrorResult
	ZAdd(key string, member interface{}, score float64) ErrorResult
	ZRem(key string, member interface{}) ErrorResult

	Exec() error
}

// FallbackBatchOperation provides a suitable fallback for stores that don't supported optimized
// batching.
type FallbackBatchOperation struct {
	Backend Backend

	fs         []func()
	firstError error
}

type fboGetResult struct {
	value *string
	err   error
}

func (r *fboGetResult) Result() (*string, error) {
	return r.value, r.err
}

func (op *FallbackBatchOperation) Get(key string) GetResult {
	result := &fboGetResult{}
	op.fs = append(op.fs, func() {
		result.value, result.err = op.Backend.Get(key)
		if result.err != nil && op.firstError == nil {
			op.firstError = result.err
		}
	})
	return result
}

type fboErrorResult struct {
	err error
}

func (r *fboErrorResult) Result() error {
	return r.err
}

func (op *FallbackBatchOperation) Set(key string, value interface{}) ErrorResult {
	result := &fboErrorResult{}
	op.fs = append(op.fs, func() {
		result.err = op.Backend.Set(key, value)
		if result.err != nil && op.firstError == nil {
			op.firstError = result.err
		}
	})
	return result
}

type fboDeleteResult struct {
	success bool
	err     error
}

func (r *fboDeleteResult) Result() (bool, error) {
	return r.success, r.err
}

func (op *FallbackBatchOperation) Delete(key string) DeleteResult {
	result := &fboDeleteResult{}
	op.fs = append(op.fs, func() {
		result.success, result.err = op.Backend.Delete(key)
		if result.err != nil && op.firstError == nil {
			op.firstError = result.err
		}
	})
	return result
}

type fboSMembersResult struct {
	value []string
	err   error
}

func (r *fboSMembersResult) Result() ([]string, error) {
	return r.value, r.err
}

func (op *FallbackBatchOperation) SMembers(key string) SMembersResult {
	result := &fboSMembersResult{}
	op.fs = append(op.fs, func() {
		result.value, result.err = op.Backend.SMembers(key)
		if result.err != nil && op.firstError == nil {
			op.firstError = result.err
		}
	})
	return result
}

func (op *FallbackBatchOperation) SAdd(key string, member interface{}, members ...interface{}) ErrorResult {
	result := &fboErrorResult{}
	op.fs = append(op.fs, func() {
		result.err = op.Backend.SAdd(key, member, members...)
		if result.err != nil && op.firstError == nil {
			op.firstError = result.err
		}
	})
	return result
}

func (op *FallbackBatchOperation) SRem(key string, member interface{}, members ...interface{}) ErrorResult {
	result := &fboErrorResult{}
	op.fs = append(op.fs, func() {
		result.err = op.Backend.SRem(key, member, members...)
		if result.err != nil && op.firstError == nil {
			op.firstError = result.err
		}
	})
	return result
}

func (op *FallbackBatchOperation) ZAdd(key string, member interface{}, score float64) ErrorResult {
	result := &fboErrorResult{}
	op.fs = append(op.fs, func() {
		result.err = op.Backend.ZAdd(key, member, score)
		if result.err != nil && op.firstError == nil {
			op.firstError = result.err
		}
	})
	return result
}

func (op *FallbackBatchOperation) ZRem(key string, member interface{}) ErrorResult {
	result := &fboErrorResult{}
	op.fs = append(op.fs, func() {
		result.err = op.Backend.ZRem(key, member)
		if result.err != nil && op.firstError == nil {
			op.firstError = result.err
		}
	})
	return result
}

func (op *FallbackBatchOperation) Exec() error {
	for _, f := range op.fs {
		f()
	}
	return op.firstError
}
