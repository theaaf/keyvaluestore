package redisstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProfiler(t *testing.T) {
	client, err := newRedisTestClient()
	if err != nil {
		t.Fatal(err)
	} else if client == nil {
		t.Skip("no redis server available")
	}

	backend := &Backend{
		Client: client,
	}

	profiler := &BasicProfiler{}
	profiled := backend.WithProfiler(profiler)

	v, err := backend.Get("foo")
	assert.Nil(t, v)
	assert.NoError(t, err)

	assert.Equal(t, 0, profiler.RedisCommandCount())
	assert.EqualValues(t, 0, profiler.RedisCommandDuration())

	v, err = profiled.Get("foo")
	assert.Nil(t, v)
	assert.NoError(t, err)

	commandCount := profiler.RedisCommandCount()
	commandDuration := profiler.RedisCommandDuration()
	assert.True(t, commandCount > 0)
	assert.True(t, commandDuration > 0)

	v, err = profiled.Get("foo")
	assert.Nil(t, v)
	assert.NoError(t, err)

	assert.True(t, profiler.RedisCommandCount() > commandCount)
	assert.True(t, profiler.RedisCommandDuration() > commandDuration)

	profiler2 := &BasicProfiler{}
	profiledTwice := profiled.WithProfiler(profiler2)

	v, err = profiledTwice.Get("foo")
	assert.Nil(t, v)
	assert.NoError(t, err)

	assert.True(t, profiler.RedisCommandCount() > commandCount)
	assert.True(t, profiler.RedisCommandDuration() > commandDuration)

	assert.True(t, profiler2.RedisCommandCount() > 0)
	assert.True(t, profiler2.RedisCommandDuration() > 0)
}

func TestProfiler_Batch(t *testing.T) {
	client, err := newRedisTestClient()
	if err != nil {
		t.Fatal(err)
	} else if client == nil {
		t.Skip("no redis server available")
	}

	backend := &Backend{
		Client: client,
	}

	profiler := &BasicProfiler{}
	profiled := backend.WithProfiler(profiler)

	batch := profiled.Batch()
	batch.Get("foo")
	require.NoError(t, batch.Exec())

	assert.True(t, profiler.RedisCommandCount() > 0)
	assert.True(t, profiler.RedisCommandDuration() > 0)
}
