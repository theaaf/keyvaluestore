package keyvaluestore

type AtomicWriteResult interface {
	// Returns false if the transaction failed due to this operation's conditional failing.
	ConditionalFailed() bool
}

// DynamoDB can't do more than 10 operations in an atomic write. So all stores should enforce this
// limit.
const MaxAtomicWriteOperations = 10

type AtomicWriteOperation interface {
	SetNX(key string, value interface{}) AtomicWriteResult
	CAS(key string, oldValue, newValue string) AtomicWriteResult
	Delete(key string) AtomicWriteResult

	// Executes the operation. If a condition failed, returns false.
	Exec() (bool, error)
}
