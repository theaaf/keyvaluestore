package keyvaluestoretest

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/theaaf/keyvaluestore"
)

type testBinaryMarshaler struct{}

func (testBinaryMarshaler) MarshalBinary() ([]byte, error) {
	return []byte("bar"), nil
}

func assertConditionPass(t *testing.T, r keyvaluestore.AtomicWriteResult) {
	assert.False(t, r.ConditionalFailed())
}

func assertConditionFail(t *testing.T, r keyvaluestore.AtomicWriteResult) {
	assert.True(t, r.ConditionalFailed())
}

func TestBackendAtomicWrite(t *testing.T, newBackend func() keyvaluestore.Backend) {
	b := newBackend()

	t.Run("SetNX", func(t *testing.T) {
		assert.NoError(t, b.Set("foo", "bar"))
		_, err := b.Delete("notset")
		assert.NoError(t, err)
		_, err = b.Delete("notset2")
		assert.NoError(t, err)

		tx := b.AtomicWrite()
		defer assertConditionFail(t, tx.SetNX("foo", "bar"))
		ok, err := tx.Exec()
		assert.NoError(t, err)
		assert.False(t, ok)

		tx = b.AtomicWrite()
		defer assertConditionPass(t, tx.SetNX("notset", "bar"))
		defer assertConditionPass(t, tx.SetNX("notset2", "bar2"))
		ok, err = tx.Exec()
		assert.NoError(t, err)
		assert.True(t, ok)

		v, err := b.Get("notset")
		require.NoError(t, err)
		assert.Equal(t, "bar", *v)
		v, err = b.Get("notset2")
		require.NoError(t, err)
		assert.Equal(t, "bar2", *v)
	})

	t.Run("Delete", func(t *testing.T) {
		assert.NoError(t, b.Set("foo", "bar"))
		assert.NoError(t, b.Set("deleteme", "bar"))
		_, err := b.Delete("notset")
		assert.NoError(t, err)

		tx := b.AtomicWrite()
		defer assertConditionFail(t, tx.SetNX("foo", "bar"))
		tx.Delete("deleteme")
		ok, err := tx.Exec()
		assert.NoError(t, err)
		assert.False(t, ok)

		got, err := b.Get("deleteme")
		assert.NoError(t, err)
		assert.NotNil(t, got)

		tx = b.AtomicWrite()
		defer assertConditionPass(t, tx.SetNX("notset", "bar"))
		tx.Delete("deleteme")
		ok, err = tx.Exec()
		assert.NoError(t, err)
		assert.True(t, ok)

		got, err = b.Get("deleteme")
		assert.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("CAS", func(t *testing.T) {
		assert.NoError(t, b.Set("foo", "bar"))
		assert.NoError(t, b.Set("deleteme", "bar"))
		_, err := b.Delete("notset")
		assert.NoError(t, err)

		tx := b.AtomicWrite()
		defer assertConditionFail(t, tx.CAS("foo", "baz", "qux"))
		defer assertConditionPass(t, tx.SetNX("notset", "bar"))
		ok, err := tx.Exec()
		assert.NoError(t, err)
		assert.False(t, ok)

		tx = b.AtomicWrite()
		defer assertConditionPass(t, tx.CAS("foo", "bar", "baz"))
		defer assertConditionPass(t, tx.SetNX("notset", "bar"))
		ok, err = tx.Exec()
		assert.NoError(t, err)
		assert.True(t, ok)
	})
}

func TestBackend(t *testing.T, newBackend func() keyvaluestore.Backend) {
	t.Run("Set", func(t *testing.T) {
		t.Run("BinaryMarshaler", func(t *testing.T) {
			b := newBackend()

			assert.NoError(t, b.Set("foo", &testBinaryMarshaler{}))

			v, err := b.Get("foo")
			require.NotNil(t, v)
			assert.NoError(t, err)
			assert.Equal(t, "bar", *v)
		})
	})

	t.Run("AddInt", func(t *testing.T) {
		b := newBackend()

		t.Run("New", func(t *testing.T) {
			n, err := b.AddInt("foo", 2)
			assert.EqualValues(t, 2, n)
			assert.NoError(t, err)

			v, err := b.Get("foo")
			require.NotNil(t, v)
			assert.NoError(t, err)
			assert.Equal(t, "2", *v)
		})

		t.Run("Existing", func(t *testing.T) {
			assert.NoError(t, b.Set("foo", 1))

			v, err := b.Get("foo")
			require.NotNil(t, v)
			assert.NoError(t, err)
			assert.Equal(t, "1", *v)

			n, err := b.AddInt("foo", 2)
			assert.EqualValues(t, 3, n)
			assert.NoError(t, err)

			v, err = b.Get("foo")
			require.NotNil(t, v)
			assert.NoError(t, err)
			assert.Equal(t, "3", *v)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		b := newBackend()

		success, err := b.Delete("foo")
		assert.False(t, success)
		assert.NoError(t, err)

		assert.NoError(t, b.Set("foo", "bar"))
		v, err := b.Get("foo")
		assert.NotNil(t, v)
		assert.NoError(t, err)

		success, err = b.Delete("foo")
		assert.NoError(t, err)
		assert.True(t, success)
		v, err = b.Get("foo")
		assert.Nil(t, v)
		assert.NoError(t, err)
	})

	t.Run("SetNX", func(t *testing.T) {
		b := newBackend()

		didSet, err := b.SetNX("foo", "bar")
		assert.True(t, didSet)
		assert.NoError(t, err)

		v, err := b.Get("foo")
		assert.NotNil(t, v)
		assert.NoError(t, err)

		didSet, err = b.SetNX("foo", "bar")
		assert.False(t, didSet)
		assert.NoError(t, err)
	})

	t.Run("SetXX", func(t *testing.T) {
		b := newBackend()

		didSet, err := b.SetXX("foo", "bar")
		assert.False(t, didSet)
		assert.NoError(t, err)

		v, err := b.Get("foo")
		assert.Nil(t, v)
		assert.NoError(t, err)

		assert.NoError(t, b.Set("foo", "x"))

		didSet, err = b.SetXX("foo", "bar")
		assert.True(t, didSet)
		assert.NoError(t, err)

		v, err = b.Get("foo")
		assert.NotNil(t, v)
		assert.Equal(t, "bar", *v)
		assert.NoError(t, err)
	})

	t.Run("SAdd", func(t *testing.T) {
		b := newBackend()

		assert.NoError(t, b.SAdd("foo", "bar"))

		members, err := b.SMembers("foo")
		assert.Equal(t, []string{"bar"}, members)
		assert.NoError(t, err)

		assert.NoError(t, b.SAdd("foo", "baz"))
		assert.NoError(t, b.SAdd("foo", "baz"))

		members, err = b.SMembers("foo")
		assert.ElementsMatch(t, []string{"bar", "baz"}, members)
		assert.NoError(t, err)

		// DynamoDB has a 400KB size limit for items. Make sure sets work fine when they grow larger
		// than that.
		t.Run("LargeSet", func(t *testing.T) {
			b := newBackend()

			bigPrefix := strings.Repeat("x", 10000)
			expected := make([]string, 90)
			for i := 0; i < len(expected); i++ {
				s := bigPrefix + strconv.FormatInt(int64(i), 10)
				expected[i] = s
				require.NoError(t, b.SAdd("foo", s))
			}

			members, err := b.SMembers("foo")
			require.NoError(t, err)
			assert.ElementsMatch(t, expected, members)

			require.NoError(t, b.SRem("foo", expected[len(expected)-1]))
			expected = expected[:len(expected)-1]

			members, err = b.SMembers("foo")
			require.NoError(t, err)
			assert.ElementsMatch(t, expected, members)

			require.NoError(t, b.SRem("foo", expected[0]))
			require.NoError(t, b.SAdd("foo", expected[len(expected)-1]))
			expected = expected[1:]

			members, err = b.SMembers("foo")
			require.NoError(t, err)
			assert.ElementsMatch(t, expected, members)
		})
	})

	t.Run("SRem", func(t *testing.T) {
		b := newBackend()

		assert.NoError(t, b.SAdd("foo", "a", "b", "c", "d"))

		members, err := b.SMembers("foo")
		assert.ElementsMatch(t, []string{"a", "b", "c", "d"}, members)
		assert.NoError(t, err)

		assert.NoError(t, b.SRem("foo", "a", "b"))

		members, err = b.SMembers("foo")
		assert.ElementsMatch(t, []string{"c", "d"}, members)
		assert.NoError(t, err)

		t.Run("Empty", func(t *testing.T) {
			b := newBackend()

			assert.NoError(t, b.SRem("foo", "x"))
		})
	})

	t.Run("AtomicWrite", func(t *testing.T) {
		TestBackendAtomicWrite(t, newBackend)
	})

	t.Run("Batch", func(t *testing.T) {
		t.Run("Get", func(t *testing.T) {
			b := newBackend()

			assert.NoError(t, b.Set("foo", "bar"))
			assert.NoError(t, b.Set("foo2", "bar2"))

			batch := b.Batch()
			get := batch.Get("foo")
			get2 := batch.Get("foo2")
			get3 := batch.Get("foo3")
			assert.NoError(t, batch.Exec())

			v, err := get.Result()
			assert.Equal(t, "bar", *v)
			assert.NoError(t, err)

			v, err = get2.Result()
			assert.Equal(t, "bar2", *v)
			assert.NoError(t, err)

			v, err = get3.Result()
			assert.Nil(t, v)
			assert.NoError(t, err)
		})

		t.Run("SMembers", func(t *testing.T) {
			b := newBackend()

			assert.NoError(t, b.SAdd("set", "a"))
			assert.NoError(t, b.SAdd("set", "b"))

			batch := b.Batch()
			smembers := batch.SMembers("set")
			require.NoError(t, batch.Exec())
			members, _ := smembers.Result()
			assert.ElementsMatch(t, []string{"a", "b"}, members)

			// DynamoDB has a 400KB size limit for items. Make sure sets work fine when they grow larger
			// than that.
			t.Run("LargeSet", func(t *testing.T) {
				b := newBackend()

				bigPrefix := strings.Repeat("x", 10000)
				expected := make([]string, 90)
				for i := 0; i < len(expected); i++ {
					s := bigPrefix + strconv.FormatInt(int64(i), 10)
					expected[i] = s
					require.NoError(t, b.SAdd("foo", s))
				}

				batch := b.Batch()
				smembers := batch.SMembers("foo")
				require.NoError(t, batch.Exec())
				members, _ := smembers.Result()
				assert.ElementsMatch(t, expected, members)
			})
		})

		t.Run("Set", func(t *testing.T) {
			b := newBackend()

			batch := b.Batch()
			batch.Set("foo", "a")
			batch.Set("foo", "b")
			require.NoError(t, batch.Exec())

			foo, err := b.Get("foo")
			require.NotNil(t, foo)
			assert.Equal(t, "b", *foo)
			assert.NoError(t, err)
		})

		t.Run("ZAdd", func(t *testing.T) {
			b := newBackend()

			batch := b.Batch()
			batch.ZAdd("foo", "a", 0.0)
			batch.ZAdd("foo", "b", 10.0)
			require.NoError(t, batch.Exec())

			members, err := b.ZRangeByScore("foo", 0.0, 100.0, 0)
			assert.Equal(t, []string{"a", "b"}, members)
			assert.NoError(t, err)

			batch = b.Batch()
			batch.ZAdd("foo", "a", 5.0)
			batch.ZAdd("foo", "a", 20.0)
			require.NoError(t, batch.Exec())

			members, err = b.ZRangeByScore("foo", 0.0, 100.0, 0)
			assert.Equal(t, []string{"b", "a"}, members)
			assert.NoError(t, err)
		})
	})

	t.Run("CAS", func(t *testing.T) {
		t.Run("Set", func(t *testing.T) {
			b := newBackend()

			assert.NoError(t, b.Set("foo", "bar"))

			success, err := b.CAS("foo", func(prev *string) (interface{}, error) {
				assert.Equal(t, "bar", *prev)
				return "baz", nil
			})
			assert.True(t, success)
			assert.NoError(t, err)

			v, err := b.Get("foo")
			require.NoError(t, err)
			assert.Equal(t, "baz", *v)
		})

		t.Run("Contention", func(t *testing.T) {
			b := newBackend()

			assert.NoError(t, b.Set("foo", "bar"))

			success, err := b.CAS("foo", func(prev *string) (interface{}, error) {
				assert.Equal(t, "bar", *prev)
				assert.NoError(t, b.Set("foo", "qux"))
				return "baz", nil
			})
			assert.False(t, success)
			assert.NoError(t, err)

			v, err := b.Get("foo")
			require.NoError(t, err)
			assert.Equal(t, "qux", *v)
		})

		t.Run("NOP", func(t *testing.T) {
			b := newBackend()

			assert.NoError(t, b.Set("foo", "bar"))

			success, err := b.CAS("foo", func(prev *string) (interface{}, error) {
				return nil, nil
			})
			assert.True(t, success)
			assert.NoError(t, err)

			v, err := b.Get("foo")
			require.NoError(t, err)
			assert.Equal(t, "bar", *v)
		})

		t.Run("Error", func(t *testing.T) {
			b := newBackend()

			assert.NoError(t, b.Set("foo", "bar"))

			success, err := b.CAS("foo", func(prev *string) (interface{}, error) {
				return nil, fmt.Errorf("err")
			})
			assert.False(t, success)
			assert.Error(t, err)

			v, err := b.Get("foo")
			require.NoError(t, err)
			assert.Equal(t, "bar", *v)
		})
	})

	t.Run("ZRem", func(t *testing.T) {
		b := newBackend()

		assert.NoError(t, b.ZAdd("foo", "a", 0.0))
		assert.NoError(t, b.ZAdd("foo", "b", 0.0))

		members, err := b.ZRangeByLex("foo", "-", "+", 0)
		assert.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, members)

		assert.NoError(t, b.ZRem("foo", "a"))

		members, err = b.ZRangeByLex("foo", "-", "+", 0)
		assert.NoError(t, err)
		assert.Equal(t, []string{"b"}, members)
	})

	t.Run("ZRangeByScore", func(t *testing.T) {
		b := newBackend()

		assert.NoError(t, b.ZAdd("foo", "-2", -2.0))
		assert.NoError(t, b.ZAdd("foo", "-1", -1.0))
		assert.NoError(t, b.ZAdd("foo", "-0.5", -0.5))
		assert.NoError(t, b.ZAdd("foo", "0", 0.0))
		assert.NoError(t, b.ZAdd("foo", "0.5", 0.5))
		assert.NoError(t, b.ZAdd("foo", "0.5b", 0.5))
		assert.NoError(t, b.ZAdd("foo", "1", 1.0))
		assert.NoError(t, b.ZAdd("foo", "2", 2.0))

		t.Run("MinMax", func(t *testing.T) {
			members, err := b.ZRangeByScore("foo", -0.5, 1.0, 0)
			assert.NoError(t, err)
			assert.Equal(t, []string{"-0.5", "0", "0.5", "0.5b", "1"}, members)
		})

		t.Run("-Inf", func(t *testing.T) {
			members, err := b.ZRangeByScore("foo", math.Inf(-1), 1, 0)
			assert.NoError(t, err)
			assert.Equal(t, []string{"-2", "-1", "-0.5", "0", "0.5", "0.5b", "1"}, members)
		})

		t.Run("+Inf", func(t *testing.T) {
			members, err := b.ZRangeByScore("foo", -0.5, math.Inf(1), 0)
			assert.NoError(t, err)
			assert.Equal(t, []string{"-0.5", "0", "0.5", "0.5b", "1", "2"}, members)
		})

		t.Run("Rev", func(t *testing.T) {
			t.Run("MinMax", func(t *testing.T) {
				members, err := b.ZRevRangeByScore("foo", -0.5, 1.0, 0)
				assert.NoError(t, err)
				assert.Equal(t, []string{"1", "0.5b", "0.5", "0", "-0.5"}, members)
			})

			t.Run("-Inf", func(t *testing.T) {
				members, err := b.ZRevRangeByScore("foo", math.Inf(-1), 1, 0)
				assert.NoError(t, err)
				assert.Equal(t, []string{"1", "0.5b", "0.5", "0", "-0.5", "-1", "-2"}, members)
			})

			t.Run("+Inf", func(t *testing.T) {
				members, err := b.ZRevRangeByScore("foo", -0.5, math.Inf(1), 0)
				assert.NoError(t, err)
				assert.Equal(t, []string{"2", "1", "0.5b", "0.5", "0", "-0.5"}, members)
			})
		})

		t.Run("Update", func(t *testing.T) {
			assert.NoError(t, b.ZAdd("update-test", "foo", 2.0))

			members, err := b.ZRangeByScore("update-test", 1.5, 2.5, 0)
			assert.NoError(t, err)
			assert.Equal(t, []string{"foo"}, members)

			assert.NoError(t, b.ZAdd("update-test", "foo", 3.0))

			members, err = b.ZRangeByScore("update-test", 1.5, 2.5, 0)
			assert.NoError(t, err)
			assert.Empty(t, members)

			members, err = b.ZRangeByScore("update-test", 2.5, 3.5, 0)
			assert.NoError(t, err)
			assert.Equal(t, []string{"foo"}, members)
		})
	})

	t.Run("ZRangeByLex", func(t *testing.T) {
		b := newBackend()

		assert.NoError(t, b.ZAdd("foo", "a", 0.0))
		assert.NoError(t, b.ZAdd("foo", "b", 0.0))
		assert.NoError(t, b.ZAdd("foo", "c", 0.0))
		assert.NoError(t, b.ZAdd("foo", "d", 0.0))

		t.Run("Inf", func(t *testing.T) {
			members, err := b.ZRangeByLex("foo", "-", "+", 0)
			assert.NoError(t, err)
			assert.Equal(t, []string{"a", "b", "c", "d"}, members)
		})

		t.Run("MinGreaterThanMax", func(t *testing.T) {
			members, err := b.ZRangeByLex("foo", "(d", "(a", 0)
			assert.NoError(t, err)
			assert.Empty(t, members)
		})

		t.Run("MinMaxExclusive", func(t *testing.T) {
			members, err := b.ZRangeByLex("foo", "(a", "(d", 0)
			assert.NoError(t, err)
			assert.Equal(t, []string{"b", "c"}, members)
		})

		t.Run("MinMaxInclusive", func(t *testing.T) {
			members, err := b.ZRangeByLex("foo", "[a", "[d", 0)
			assert.NoError(t, err)
			assert.Equal(t, []string{"a", "b", "c", "d"}, members)
		})

		t.Run("RangeInclusive", func(t *testing.T) {
			members, err := b.ZRangeByLex("foo", "[b", "[c", 0)
			assert.NoError(t, err)
			assert.Equal(t, []string{"b", "c"}, members)
		})

		t.Run("SingleElement", func(t *testing.T) {
			members, err := b.ZRangeByLex("foo", "[b", "[b", 0)
			assert.NoError(t, err)
			assert.Equal(t, []string{"b"}, members)
		})

		t.Run("SingleAbsentElement", func(t *testing.T) {
			members, err := b.ZRangeByLex("foo", "[z", "[z", 1)
			assert.NoError(t, err)
			assert.Empty(t, members)
		})

		t.Run("Rev", func(t *testing.T) {
			t.Run("Inf", func(t *testing.T) {
				members, err := b.ZRevRangeByLex("foo", "-", "+", 0)
				assert.NoError(t, err)
				assert.Equal(t, []string{"d", "c", "b", "a"}, members)
			})

			t.Run("MinMaxExclusive", func(t *testing.T) {
				members, err := b.ZRevRangeByLex("foo", "(a", "(d", 0)
				assert.NoError(t, err)
				assert.Equal(t, []string{"c", "b"}, members)
			})

			t.Run("MinMaxInclusive", func(t *testing.T) {
				members, err := b.ZRevRangeByLex("foo", "[a", "[d", 0)
				assert.NoError(t, err)
				assert.Equal(t, []string{"d", "c", "b", "a"}, members)
			})

			t.Run("RangeInclusive", func(t *testing.T) {
				members, err := b.ZRevRangeByLex("foo", "[b", "[c", 0)
				assert.NoError(t, err)
				assert.Equal(t, []string{"c", "b"}, members)
			})

			t.Run("SingleAbsentElement", func(t *testing.T) {
				members, err := b.ZRangeByLex("foo", "[z", "[z", 1)
				assert.NoError(t, err)
				assert.Empty(t, members)
			})
		})
	})

	t.Run("ZScore", func(t *testing.T) {
		b := newBackend()

		assert.NoError(t, b.ZAdd("foo", "a", 0.0))
		assert.NoError(t, b.ZAdd("foo", "b", 1.0))

		zeroF := 0.0
		oneF := 1.0
		for _, tc := range []struct {
			member   string
			expected *float64
		}{
			{"a", &zeroF},
			{"b", &oneF},
			{"c", nil},
		} {
			score, err := b.ZScore("foo", tc.member)
			assert.NoError(t, err)
			if tc.expected == nil {
				assert.Nil(t, score)
			} else {
				if assert.NotNil(t, score) {
					assert.Equal(t, *tc.expected, *score)
				}
			}
		}
	})

	t.Run("ZCount", func(t *testing.T) {
		b := newBackend()

		assert.NoError(t, b.ZAdd("foo", "a", 0.0))
		assert.NoError(t, b.ZAdd("foo", "b", 1.0))
		assert.NoError(t, b.ZAdd("foo", "c", 2.0))
		assert.NoError(t, b.ZAdd("foo", "d", 3.0))
		assert.NoError(t, b.ZAdd("foo", "e", 4.0))
		assert.NoError(t, b.ZAdd("foo", "f", 5.0))

		for _, tc := range []struct {
			min, max float64
			expected int
		}{
			{1.0, 2.0, 2},
			{1.0, 1.5, 1},
			{math.Inf(-1), 2, 3},
			{math.Inf(-1), math.Inf(1), 6},
			{2.0, math.Inf(1), 4},
		} {
			n, err := b.ZCount("foo", tc.min, tc.max)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, n, fmt.Sprintf("%#v %#v", tc.min, tc.max))
		}
	})

	t.Run("ZLexCount", func(t *testing.T) {
		b := newBackend()

		assert.NoError(t, b.ZAdd("foo", "a", 0.0))
		assert.NoError(t, b.ZAdd("foo", "c", 0.0))
		assert.NoError(t, b.ZAdd("foo", "e", 0.0))
		assert.NoError(t, b.ZAdd("foo", "g", 0.0))

		for _, tc := range []struct {
			min, max string
			expected int
		}{
			{"[g", "[g", 1},
			{"[a", "[g", 4},
			{"(a", "[g", 3},
			{"[a", "(g", 3},
			{"[c", "[e", 2},
			{"[e", "(g", 1},
			{"(a", "[e", 2},
			{"[e", "[e", 1},
			{"[f", "[f", 0},
			{"[_", "[g", 4},
			{"[a", "[h", 4},
			{"-", "[e", 3},
			{"[c", "+", 3},
			{"-", "+", 4},
			{"[a", "(e", 2},
			{"[a", "(f", 3},
		} {
			n, err := b.ZLexCount("foo", tc.min, tc.max)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, n, fmt.Sprintf("%#v %#v", tc.min, tc.max))
		}
	})

	t.Run("ZIncrBy", func(t *testing.T) {
		b := newBackend()

		t.Run("ExistingKey", func(t *testing.T) {
			assert.NoError(t, b.ZAdd("existing", "abc", 0.5))

			newVal, err := b.ZIncrBy("existing", "abc", 1)
			require.NoError(t, err)

			assert.EqualValues(t, 1.5, newVal)

			vals, err := b.ZRangeByScore("existing", 1.5, 1.5, 10)
			require.NoError(t, err)

			assert.Equal(t, []string{"abc"}, vals)

			vals, err = b.ZRangeByScore("existing", 0, 1, 10)
			require.NoError(t, err)

			assert.Empty(t, vals)
		})

		t.Run("NoExistingKey", func(t *testing.T) {
			newVal, err := b.ZIncrBy("missing", "bcd", 1)
			require.NoError(t, err)

			assert.EqualValues(t, 1, newVal)

			vals, err := b.ZRangeByScore("missing", 1, 1, 10)
			require.NoError(t, err)

			assert.Equal(t, []string{"bcd"}, vals)
		})

		t.Run("Negative", func(t *testing.T) {
			assert.NoError(t, b.ZAdd("neg", "cde", 0.5))

			newVal, err := b.ZIncrBy("neg", "cde", -1)
			require.NoError(t, err)

			assert.EqualValues(t, -0.5, newVal)

			vals, err := b.ZRangeByScore("neg", -0.5, -0.5, 10)
			require.NoError(t, err)

			assert.Equal(t, []string{"cde"}, vals)

			vals, err = b.ZRangeByScore("neg", 0, 1, 10)
			require.NoError(t, err)

			assert.Empty(t, vals)
		})

		t.Run("MultipleWriters", func(t *testing.T) {
			outerLoops := 10
			innerLoops := 10
			var wg sync.WaitGroup

			for i := 0; i < outerLoops; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					successful := 0
					for successful < innerLoops {
						_, err := b.ZIncrBy("MultipleWriters", "foo", 1)

						if err == nil {
							successful++
						}
					}
				}()
			}

			wg.Wait()

			vals, err := b.ZRangeByScore("MultipleWriters", float64(outerLoops*innerLoops), float64(outerLoops*innerLoops), 10)
			require.NoError(t, err)
			assert.Equal(t, []string{"foo"}, vals)
		})
	})

	t.Run("ZRangeByScoreWithScores", func(t *testing.T) {
		b := newBackend()

		assert.NoError(t, b.ZAdd("foo", "-2", -2.0))
		assert.NoError(t, b.ZAdd("foo", "-1", -1.0))
		assert.NoError(t, b.ZAdd("foo", "-0.5", -0.5))
		assert.NoError(t, b.ZAdd("foo", "0", 0.0))
		assert.NoError(t, b.ZAdd("foo", "0.5", 0.5))
		assert.NoError(t, b.ZAdd("foo", "0.5b", 0.5))
		assert.NoError(t, b.ZAdd("foo", "1", 1.0))
		assert.NoError(t, b.ZAdd("foo", "2", 2.0))

		t.Run("MinMax", func(t *testing.T) {
			members, err := b.ZRangeByScoreWithScores("foo", -0.5, 1.0, 0)
			assert.NoError(t, err)
			assert.Equal(t, keyvaluestore.ScoredMembers{
				{Score: -0.5, Value: "-0.5"},
				{Score: 0, Value: "0"},
				{Score: 0.5, Value: "0.5"},
				{Score: 0.5, Value: "0.5b"},
				{Score: 1, Value: "1"},
			}, members)
		})

		t.Run("-Inf", func(t *testing.T) {
			members, err := b.ZRangeByScoreWithScores("foo", math.Inf(-1), 1, 0)
			assert.NoError(t, err)
			assert.Equal(t, keyvaluestore.ScoredMembers{
				{Score: -2, Value: "-2"},
				{Score: -1, Value: "-1"},
				{Score: -0.5, Value: "-0.5"},
				{Score: 0, Value: "0"},
				{Score: 0.5, Value: "0.5"},
				{Score: 0.5, Value: "0.5b"},
				{Score: 1, Value: "1"},
			}, members)
		})

		t.Run("+Inf", func(t *testing.T) {
			members, err := b.ZRangeByScoreWithScores("foo", -0.5, math.Inf(1), 0)
			assert.NoError(t, err)
			assert.Equal(t, keyvaluestore.ScoredMembers{
				{Score: -0.5, Value: "-0.5"},
				{Score: 0, Value: "0"},
				{Score: 0.5, Value: "0.5"},
				{Score: 0.5, Value: "0.5b"},
				{Score: 1, Value: "1"},
				{Score: 2, Value: "2"},
			}, members)
		})
	})

	t.Run("ZRevRangeByScoreWithScores", func(t *testing.T) {
		b := newBackend()

		assert.NoError(t, b.ZAdd("foo", "-2", -2.0))
		assert.NoError(t, b.ZAdd("foo", "-1", -1.0))
		assert.NoError(t, b.ZAdd("foo", "-0.5", -0.5))
		assert.NoError(t, b.ZAdd("foo", "0", 0.0))
		assert.NoError(t, b.ZAdd("foo", "0.5", 0.5))
		assert.NoError(t, b.ZAdd("foo", "0.5b", 0.5))
		assert.NoError(t, b.ZAdd("foo", "1", 1.0))
		assert.NoError(t, b.ZAdd("foo", "2", 2.0))

		t.Run("MinMax", func(t *testing.T) {
			members, err := b.ZRevRangeByScoreWithScores("foo", -0.5, 1.0, 0)
			assert.NoError(t, err)
			assert.Equal(t, keyvaluestore.ScoredMembers{
				{Score: 1, Value: "1"},
				{Score: 0.5, Value: "0.5b"},
				{Score: 0.5, Value: "0.5"},
				{Score: 0, Value: "0"},
				{Score: -0.5, Value: "-0.5"},
			}, members)
		})

		t.Run("-Inf", func(t *testing.T) {
			members, err := b.ZRevRangeByScoreWithScores("foo", math.Inf(-1), 1, 0)
			assert.NoError(t, err)
			assert.Equal(t, keyvaluestore.ScoredMembers{
				{Score: 1, Value: "1"},
				{Score: 0.5, Value: "0.5b"},
				{Score: 0.5, Value: "0.5"},
				{Score: 0, Value: "0"},
				{Score: -0.5, Value: "-0.5"},
				{Score: -1, Value: "-1"},
				{Score: -2, Value: "-2"},
			}, members)
		})

		t.Run("+Inf", func(t *testing.T) {
			members, err := b.ZRevRangeByScoreWithScores("foo", -0.5, math.Inf(1), 0)
			assert.NoError(t, err)
			assert.Equal(t, keyvaluestore.ScoredMembers{
				{Score: 2, Value: "2"},
				{Score: 1, Value: "1"},
				{Score: 0.5, Value: "0.5b"},
				{Score: 0.5, Value: "0.5"},
				{Score: 0, Value: "0"},
				{Score: -0.5, Value: "-0.5"},
			}, members)
		})
	})
}
