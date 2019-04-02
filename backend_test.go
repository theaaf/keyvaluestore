package keyvaluestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScoredMembers_Values(t *testing.T) {
	t.Run("NotNil", func(t *testing.T) {
		members := ScoredMembers{}
		members = append(members, &ScoredMember{Value: "foo", Score: 3})

		assert.Equal(t, []string{"foo"}, members.Values())
	})

	t.Run("Empty", func(t *testing.T) {
		var members ScoredMembers
		assert.Equal(t, []string{}, members.Values())
	})
}
