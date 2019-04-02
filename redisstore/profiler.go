package redisstore

import (
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
)

type Profiler interface {
	AddRedisCommandProfile(cmd redis.Cmder, duration time.Duration)
	AddRedisPipelineProfile(cmds []redis.Cmder, duration time.Duration)
}

type BasicProfiler struct {
	redisCommandCount       int64
	redisRoundTripCount     int64
	redisCommandNanoseconds int64
}

var _ Profiler = (*BasicProfiler)(nil)

func (p *BasicProfiler) AddRedisCommandProfile(cmd redis.Cmder, duration time.Duration) {
	atomic.AddInt64(&p.redisCommandCount, 1)
	atomic.AddInt64(&p.redisRoundTripCount, 1)
	atomic.AddInt64(&p.redisCommandNanoseconds, int64(duration/time.Nanosecond))
}

func (p *BasicProfiler) AddRedisPipelineProfile(cmds []redis.Cmder, duration time.Duration) {
	atomic.AddInt64(&p.redisCommandCount, int64(len(cmds)))
	atomic.AddInt64(&p.redisRoundTripCount, 1)
	atomic.AddInt64(&p.redisCommandNanoseconds, int64(duration/time.Nanosecond))
}

func (p *BasicProfiler) RedisCommandCount() int {
	return int(atomic.LoadInt64(&p.redisCommandCount))
}

func (p *BasicProfiler) RedisRoundTripCount() int {
	return int(atomic.LoadInt64(&p.redisRoundTripCount))
}

func (p *BasicProfiler) RedisCommandDuration() time.Duration {
	return time.Duration(atomic.LoadInt64(&p.redisCommandNanoseconds)) * time.Nanosecond
}

func ProfileClient(client *redis.Client, profiler Profiler) *redis.Client {
	ret := client.WithContext(client.Context())
	ret.WrapProcess(func(old func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			start := time.Now()
			err := old(cmd)
			profiler.AddRedisCommandProfile(cmd, time.Since(start))
			return err
		}
	})
	ret.WrapProcessPipeline(func(old func(cmds []redis.Cmder) error) func(cmds []redis.Cmder) error {
		return func(cmds []redis.Cmder) error {
			start := time.Now()
			err := old(cmds)
			profiler.AddRedisPipelineProfile(cmds, time.Since(start))
			return err
		}
	})
	return ret
}
