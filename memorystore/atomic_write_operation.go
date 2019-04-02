package memorystore

import (
	"fmt"

	"github.com/theaaf/keyvaluestore"
)

type AtomicWriteOperation struct {
	Backend *Backend

	operations []*atomicWriteOperation
}

type atomicWriteOperation struct {
	condition func() bool
	write     func()

	conditionPassed bool
}

func (op *atomicWriteOperation) ConditionalFailed() bool {
	return !op.conditionPassed
}

func (op *AtomicWriteOperation) write(wOp *atomicWriteOperation) keyvaluestore.AtomicWriteResult {
	op.operations = append(op.operations, wOp)
	return wOp
}

func (op *AtomicWriteOperation) SetNX(key string, value interface{}) keyvaluestore.AtomicWriteResult {
	return op.write(&atomicWriteOperation{
		condition: func() bool {
			return op.Backend.get(key) == nil
		},
		write: func() {
			op.Backend.set(key, value)
		},
	})
}

func (op *AtomicWriteOperation) CAS(key string, oldValue, newValue string) keyvaluestore.AtomicWriteResult {
	return op.write(&atomicWriteOperation{
		condition: func() bool {
			v := op.Backend.get(key)
			return v != nil && *v == oldValue
		},
		write: func() {
			op.Backend.set(key, newValue)
		},
	})
}

func (op *AtomicWriteOperation) Delete(key string) keyvaluestore.AtomicWriteResult {
	return op.write(&atomicWriteOperation{
		write: func() {
			op.Backend.delete(key)
		},
	})
}

func (op *AtomicWriteOperation) Exec() (bool, error) {
	if len(op.operations) > keyvaluestore.MaxAtomicWriteOperations {
		return false, fmt.Errorf("max operation count exceeded")
	}

	op.Backend.mutex.Lock()
	defer op.Backend.mutex.Unlock()

	allPassed := true

	for _, wOp := range op.operations {
		if wOp.condition == nil {
			wOp.conditionPassed = true
		} else {
			pass := wOp.condition()
			wOp.conditionPassed = pass
			if !pass {
				allPassed = false
			}
		}
	}

	if !allPassed {
		return false, nil
	}

	for _, wOp := range op.operations {
		wOp.write()
	}

	return true, nil
}
