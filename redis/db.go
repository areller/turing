package redis

import (
	"strings"
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
	Set(key string, value interface{}) error
	Get(key string) (string, error)
	Delete(keys ...string) int
	Exists(key string) bool
	HSet(key string, field string, value interface{}) error
	HSetMany(key string, kv map[string]interface{}) error
	HGet(key string, field string) (string, error)
	HGetAll(key string) (map[string]string, error)
	HDelete(key string, fields []string) (int, error)
	SetWithExpiry(key string, value interface{}, expiry time.Duration) error
	Expire(key string, expiry time.Duration) error
	LPush(key string, values ...interface{}) error
	LTrim(key string, start int64, end int64) error
	RPush(key string, value ...interface{}) error
	LPop(key string) (string, error)
	RPop(key string) (string, error)
	LRange(key string, start int64, end int64) ([]string, error)
	Publish(key string, value interface{}) int
	IncreaseBy(key string, value float64) (float64, error)
	SAdd(key string, values ...interface{}) (int64, error)
	SCard(key string) (int64, error)
	SIsMember(key string, value interface{}) (bool, error)
	SMembers(key string) ([]string, error)
	SRem(key string, values ...interface{}) (int64, error)
}

type RDB struct {
	native *redis.Client
}

func (rdb *RDB) mustRun(key string, cmd string, fn func () (interface{}, error)) (interface{}, error) {
	res, err := fn()
	for err != nil {
		if err == redis.Nil {
			return nil, turing.KeyNotExistsError
		} else if strings.Contains(err.Error(), "WRONGTYPE") {
			return nil, turing.WrongTypeError
		} else if strings.Contains(err.Error(), "ERR") {
			return nil, turing.GeneralError
		} else {
			time.Sleep(time.Second)
			res, err = fn()
		}
	}

	return res, nil
}

func (rdb *RDB) Set(key string, value interface{}) error {
	return turing.GetError(rdb.mustRun(key, "set", func () (interface{}, error) {
		return rdb.native.Set(key, value, -1).Result()
	}))
}

func (rdb *RDB) SetWithExpiry(key string, value interface{}, expiry time.Duration) error {
	return turing.GetError(rdb.mustRun(key, "set", func () (interface{}, error) {
		return rdb.native.Set(key, value, expiry).Result()
	}))
}

func (rdb *RDB) Expire(key string, expiry time.Duration) error {
	res, err := rdb.mustRun(key, "expire", func () (interface{}, error) {
		return rdb.native.Expire(key, expiry).Result()
	})
	if err != nil {
		return err
	} else if !res.(bool) {
		return turing.KeyNotExistsError
	} else {
		return nil
	}
}

func (rdb *RDB) Get(key string) (string, error) {
	res, err := rdb.mustRun(key, "get", func () (interface{}, error) {
		return rdb.native.Get(key).Result()
	})
	return res.(string), err
}

func (rdb *RDB) Delete(keys ...string) int {
	num, _ := rdb.mustRun("", "del", func () (interface{}, error) {
		return rdb.native.Del(keys...).Result()
	})
	return num.(int)
}

func (rdb *RDB) Exists(key string) bool {
	res, _ := rdb.mustRun(key, "exists", func () (interface{}, error) {
		return rdb.native.Exists(key).Result()
	})
	return res.(int) == 1
}

func (rdb *RDB) HSet(key string, field string, value interface{}) error {
	return turing.GetError(rdb.mustRun(key, "hset", func () (interface{}, error) {
		return rdb.native.HSet(key, field, value).Result()
	}))
}

func (rdb *RDB) HSetMany(key string, kv map[string]interface{}) error {
	return turing.GetError(rdb.mustRun(key, "hmset", func () (interface{}, error) {
		return rdb.native.HMSet(key, kv).Result()
	}))
}

func (rdb *RDB) HGet(key string, field string) (string, error) {
	res, err := rdb.mustRun(key, "hget", func () (interface{}, error) {
		return rdb.native.HGet(key, field).Result()
	})
	return res.(string), err
}

func (rdb *RDB) HGetAll(key string) (map[string]string, error) {
	res, err := rdb.mustRun(key, "hgetall", func () (interface{}, error) {
		return rdb.native.HGetAll(key).Result()
	})
	return res.(map[string]string), err
}

func (rdb *RDB) HDelete(key string, fields []string) (int, error) {
	res, err := rdb.mustRun(key, "hdel", func () (interface{}, error) {
		return rdb.native.HDel(key, fields...).Result()
	})
	return res.(int), err
}

func (rdb *RDB) IncreaseBy(key string, value float64) (float64, error) {
	res, err := rdb.mustRun(key, "incrby", func () (interface{}, error) {
		return rdb.native.IncrByFloat(key, value).Result()
	})
	return res.(float64), err
}

func (rdb *RDB) Publish(key string, value interface{}) int {
	res, _ := rdb.mustRun(key, "publish", func () (interface{}, error) {
		return rdb.native.Publish(key, value).Result()
	})
	return res.(int)
}

func (rdb *RDB) LPush(key string, values ...interface{}) error {
	return turing.GetError(rdb.mustRun(key, "lpush", func () (interface{}, error) {
		return rdb.native.LPush(key, values...).Result()
	}))
}

func (rdb *RDB) LTrim(key string, start int64, end int64) error {
	return turing.GetError(rdb.mustRun(key, "ltrim", func () (interface{}, error) {
		return rdb.native.LTrim(key, start, end).Result()
	}))
}

func (rdb *RDB) RPush(key string, values ...interface{}) error {
	return turing.GetError(rdb.mustRun(key, "rpush", func () (interface{}, error) {
		return rdb.native.RPush(key, values...).Result()
	}))
}

func (rdb *RDB) LRange(key string, start int64, end int64) ([]string, error) {
	res, err := rdb.mustRun(key, "lrange", func () (interface{}, error) {
		return rdb.native.LRange(key, start, end).Result()
	})
	return res.([]string), err
}

func (rdb *RDB) LPop(key string) (string, error) {
	res, err := rdb.mustRun(key, "lpop", func () (interface{}, error) {
		return rdb.native.LPop(key).Result()
	})
	return res.(string), err
}

func (rdb *RDB) RPop(key string) (string, error) {
	res, err := rdb.mustRun(key, "rpop", func () (interface{}, error) {
		return rdb.native.RPop(key).Result()
	})
	return res.(string), err
}

func (rdb *RDB) SAdd(key string, values ...interface{}) (int64, error) {
	res, err := rdb.mustRun(key, "sadd", func () (interface{}, error) {
		return rdb.native.SAdd(key, values...).Result()
	})
	return res.(int64), err
}

func (rdb *RDB) SCard(key string) (int64, error) {
	res, err := rdb.mustRun(key, "scard", func () (interface{}, error) {
		return rdb.native.SCard(key).Result()
	})
	return res.(int64), err
}

func NewDB(config RedisConfig) (*RDB, error) {
	native := redis.NewClient(&redis.Options{
		Addr: config.Host + ":" + strconv.Itoa(config.Port),
		Password: config.Password,
		DB: config.DB,
	})

	err := native.Ping().Err()
	return &RDB{
		native: native,
	}, err 
}

func MustNewDB(config RedisConfig) *RDB {
	return turing.MustGet(NewDB(config)).(*RDB)
}