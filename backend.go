package keyvaluestore

type Backend interface {
	// Batch allows you to batch up simple operations for better performance potential. Use this
	// only for possible performance benefits. Read isolation is implementation-defined and other
	// properties such as atomicity should not be assumed.
	Batch() BatchOperation

	// AtomicWrite executes up to 10 write operations atomically, failing entirely if any
	// conditional operations (e.g. SetNX) are not executed.
	AtomicWrite() AtomicWriteOperation

	Delete(key string) (success bool, err error)
	Get(key string) (*string, error)
	Set(key string, value interface{}) error

	// CAS performs a compare-and-swap operation. It gets the given key, allows you to transform its
	// value, then updates it only if it hasn't changed. Returning nil from the transform function
	// performs no action, causing CAS to return true, nil.
	CAS(key string, transform func(v *string) (interface{}, error)) (success bool, err error)

	// Add an integer to an integer value. Or set if the key doesn't exist.
	AddInt(key string, n int64) (int64, error)

	// Set if the key already exists.
	SetXX(key string, value interface{}) (bool, error)

	// Set if the key doesn't exist.
	SetNX(key string, value interface{}) (bool, error)

	// Add to or create a set. Sets are ideal for small sizes and fast read access. Sorted sets
	// should be considered instead for large, write-heavy applications.
	SAdd(key string, member interface{}, members ...interface{}) error

	// Remove from a set.
	SRem(key string, member interface{}, members ...interface{}) error

	// Get members of a set.
	SMembers(key string) ([]string, error)

	// Add to or create a sorted set.
	ZAdd(key string, member interface{}, score float64) error

	// Gets the score for a member added via ZAdd.
	ZScore(key string, member interface{}) (*float64, error)

	// Remove from a sorted set.
	ZRem(key string, member interface{}) error

	// Increment a score in a sorted set or set the score if the member doesn't exist.
	ZIncrBy(key string, member string, n float64) (float64, error)

	// Get members of a sorted set by ascending score.
	ZRangeByScore(key string, min, max float64, limit int) ([]string, error)

	// Get members (and their scores) of a sorted set by ascending score.
	ZRangeByScoreWithScores(key string, min, max float64, limit int) (ScoredMembers, error)

	// Get members of a sorted set by descending score.
	ZRevRangeByScore(key string, min, max float64, limit int) ([]string, error)

	// Get members (and their scores) of a sorted set by descending score.
	ZRevRangeByScoreWithScores(key string, min, max float64, limit int) (ScoredMembers, error)

	// Gets the number of members with scores between min and max, inclusive.
	ZCount(key string, min, max float64) (int, error)

	// Gets the number of members between min and max. All members of the set must have been added
	// with a zero score. min and max must begin with '(' or '[' to indicate exclusive or inclusive.
	// Alternatively, min can be "-" and max can be "+" to represent infinities.
	ZLexCount(key string, min, max string) (int, error)

	// Get members of a sorted set by lexicographical order. All members of the set must have been
	// added with a zero score. min and max must begin with '(' or '[' to indicate exclusive or
	// inclusive. Alternatively, min can be "-" and max can be "+" to represent infinities.
	ZRangeByLex(key string, min, max string, limit int) ([]string, error)

	// Get members of a sorted set by reverse lexicographical order. All members of the set must
	// have been added with a zero score. min and max must begin with '(' or '[' to indicate
	// exclusive or inclusive. Alternatively, min can be "-" and max can be "+" to represent
	// infinities.
	ZRevRangeByLex(key string, min, max string, limit int) ([]string, error)
}

type ScoredMembers []*ScoredMember

func (m ScoredMembers) Values() []string {
	result := make([]string, len(m))

	for i, member := range m {
		result[i] = member.Value
	}

	return result
}

type ScoredMember struct {
	Score float64
	Value string
}
