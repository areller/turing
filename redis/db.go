package redis

import (
	"time"
	"strconv"
	"github.com/go-redis/redis"
	"github.com/areller/turing"
)

type RedisConfig struct {
	Host string
	Port int
	DB int
	Password string
}

func RedisConfigFromTable(table turing.ConfigTable) RedisConfig {
	return RedisConfig{
		Host: table.GetString("redis_host"),
		Port: table.GetInt("redis_port"),
		DB: table.GetInt("redis_db"),
		Password: table.GetString("redis_pass"),
	}
}

type DB interface {
	Set(key string, value interface{})
	SetWithExpiry(key string, value interface{}, expiry time.Duration)
	HSet(key string, field string, value interface{})
	HSetMany(key string, kv map[string]interface{})
	LPush(key string, value interface{})
	RPush(key string, value interface{})
	IncreaseBy(key string, value float64) float64
	Get(key string) (string, bool)
	HGet(key string, field string) (string, bool)
	HGetAll(key string) map[string]string
	LPop(key string) (string, bool)
	RPop(key string) (string, bool)
	LRange(key string, start int64, end int64) []string
	Publish(key string, value interface{})
}

type RDB struct {
	native *redis.Client
}

func (rdb *RDB) mustRun(key string, cmd string, fn func () (interface{}, error)) (interface{}, bool) {
	res, err := fn()
	for err != nil {
		if err.Error() == "redis: nil" {
			return nil, false
		}
		//TODO: Log
		time.Sleep(time.Second)
		res, err = fn()
	}

	return res, true
}

func (rdb *RDB) Set(key string, value interface{}) {
	rdb.mustRun(key, "set", func () (interface{}, error) {
		return rdb.native.Set(key, value, -1).Result()
	})
}

func (rdb *RDB) SetWithExpiry(key string, value interface{}, expiry time.Duration) {
	rdb.mustRun(key, "set", func () (interface{}, error) {
		return rdb.native.Set(key, value, expiry).Result()
	})
}

func (rdb *RDB) Get(key string) (string, bool) {
	res, ex := rdb.mustRun(key, "get", func () (interface{}, error) {
		return rdb.native.Get(key).Result()
	})
	return res.(string), ex
}

func (rdb *RDB) HSet(key string, field string, value interface{}) {
	rdb.mustRun(key, "hset", func () (interface{}, error) {
		return rdb.native.HSet(key, field, value).Result()
	})
}

func (rdb *RDB) HSetMany(key string, kv map[string]interface{}) {
	rdb.mustRun(key, "hmset", func () (interface{}, error) {
		return rdb.native.HMSet(key, kv).Result()
	})
}

func (rdb *RDB) HGet(key string, field string) (string, bool) {
	res, ex := rdb.mustRun(key, "hget", func () (interface{}, error) {
		return rdb.native.HGet(key, field).Result()
	})
	return res.(string), ex
}

func (rdb *RDB) HGetAll(key string) map[string]string {
	res, _ := rdb.mustRun(key, "hgetall", func () (interface{}, error) {
		return rdb.native.HGetAll(key).Result()
	})
	return res.(map[string]string)
}

func (rdb *RDB) LPush(key string, value interface{}) {
	rdb.mustRun(key, "lpush", func () (interface{}, error) {
		return rdb.native.LPush(key, value).Result()
	})
}

func (rdb *RDB) RPush(key string, value interface{}) {
	rdb.mustRun(key, "rpush", func () (interface{}, error) {
		return rdb.native.RPush(key, value).Result()
	})
}

func (rdb *RDB) LPop(key string) (string, bool) {
	res, ex := rdb.mustRun(key, "lpop", func () (interface{}, error) {
		return rdb.native.LPop(key).Result()
	})
	return res.(string), ex
}

func (rdb *RDB) RPop(key string) (string, bool) {
	res, ex := rdb.mustRun(key, "rpop", func () (interface{}, error) {
		return rdb.native.RPop(key).Result()
	})
	return res.(string), ex
}

func (rdb *RDB) Publish(key string, value interface{}) {
	rdb.mustRun(key, "publish", func () (interface{}, error) {
		return rdb.native.Publish(key, value).Result()
	})
}

func (rdb *RDB) LRange(key string, start int64, end int64) []string {
	res, _ := rdb.mustRun(key, "lrange", func () (interface{}, error) {
		return rdb.native.LRange(key, start, end).Result()
	})
	return res.([]string)
}

func (rdb *RDB) IncreaseBy(key string, value float64) float64 {
	res, _ := rdb.mustRun(key, "incrby", func () (interface{}, error) {
		return rdb.native.IncrByFloat(key, value).Result()
	})
	return res.(float64)
}

func NewDB(config RedisConfig) (*RDB, error) {
	native := redis.NewClient(&redis.Options{
		Addr: config.Host + ":" + strconv.Itoa(config.Port),
		Password: config.Password,
		DB: config.DB,
	})

	return &RDB{
		native: native,
	}, nil
}

func MustNewDB(config RedisConfig) *RDB {
	redis, err := NewDB(config)
	if err != nil {
		panic(err)
	}

	return redis
}