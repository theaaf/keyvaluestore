package redisstore

import (
	"os"
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"

	"github.com/theaaf/keyvaluestore"
	"github.com/theaaf/keyvaluestore/keyvaluestoretest"
)

func newRedisTestClient() (*redis.Client, error) {
	var client *redis.Client
	if addr := os.Getenv("REDIS_ADDRESS"); addr != "" {
		client = redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   1,
		})
	} else {
		client = redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:6379",
			DB:   1,
		})
		if err := client.Ping().Err(); err != nil {
			return nil, nil
		}
	}
	if client != nil {
		client.FlushDB()
	}
	return client, nil
}

func TestBackend(t *testing.T) {
	client, err := newRedisTestClient()
	if err != nil {
		t.Fatal(err)
	} else if client == nil {
		t.Skip("no redis server available")
	}
	keyvaluestoretest.TestBackend(t, func() keyvaluestore.Backend {
		assert.NoError(t, client.FlushDB().Err())
		return &Backend{
			Client: client,
		}
	})
}
