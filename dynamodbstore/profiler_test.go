package dynamodbstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProfiler(t *testing.T) {
	client, err := newDynamoDBTestClient()
	if err != nil {
		t.Fatal(err)
	} else if client == nil {
		t.Skip("no dynamodb server available")
	}

	backend := newTestBackend(client, "TestProfiler")

	profiler := &BasicProfiler{}
	withProfiler := backend.WithProfiler(profiler)

	assert.NoError(t, withProfiler.Set("foo", "bar"))
	assert.Equal(t, 1, profiler.DynamoDBRequestCount())

	profiler2 := &BasicProfiler{}
	withNestedProfiler := withProfiler.WithProfiler(profiler2)

	assert.NoError(t, withNestedProfiler.Set("foo", "bar"))
	assert.Equal(t, 2, profiler.DynamoDBRequestCount())
	assert.Equal(t, 1, profiler2.DynamoDBRequestCount())

	assert.NoError(t, withProfiler.Set("foo", "bar"))
	assert.Equal(t, 3, profiler.DynamoDBRequestCount())
	assert.Equal(t, 1, profiler2.DynamoDBRequestCount())
}
