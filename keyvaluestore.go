package keyvaluestore

import (
	"encoding"
	"strconv"
)

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
