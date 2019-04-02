package keyvaluestore

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/vmihailenco/msgpack"
)

func concatKeys(b ...[]byte) []byte {
	l := 0
	for _, b := range b {
		l += 8 + len(b)
	}
	ret := make([]byte, l)
	dest := ret
	for _, b := range b {
		binary.BigEndian.PutUint64(dest, uint64(len(b)))
		if len(b) > 0 {
			copy(dest[8:], b)
		}
		dest = dest[8+len(b):]
	}
	return ret
}

// Returns a string where the first 8 bytes represent the given time in a way that can be
// lexicographically sorted, and the remaining bytes contain the id.
func timeBasedKey(t time.Time, id string) string {
	var nano int64
	if t.Unix() > 9151488000 {
		nano = 0x7fffffffffffffff
	} else if t.Unix() > 0 {
		nano = t.UnixNano()
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(nano))
	return string(buf[:]) + id
}

func ToString(v interface{}) *string {
	switch v := v.(type) {
	case int:
		s := strconv.FormatInt(int64(v), 10)
		return &s
	case int64:
		s := strconv.FormatInt(v, 10)
		return &s
	case string:
		return &v
	case []byte:
		s := string(v)
		return &s
	case encoding.BinaryMarshaler:
		if b, err := v.MarshalBinary(); err == nil {
			return ToString(b)
		}
	}
	return nil
}

func serialize(v interface{}) (string, error) {
	b, err := msgpack.Marshal(v)
	if err != nil {
		return "", err
	}
	if len(b) > 200*1024 {
		return "", fmt.Errorf("%T serialization too large", v)
	}
	return string(b), nil
}

func deserialize(s string, dest interface{}) error {
	return msgpack.Unmarshal([]byte(s), dest)
}

func timeScore(t time.Time) float64 {
	if t.Unix() > 9151488000 {
		return math.Inf(1)
	} else if t.Unix() > 0 {
		return float64(t.UnixNano() / 1000000)
	}
	return math.Inf(-1)
}
