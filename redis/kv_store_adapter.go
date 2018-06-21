package redis

import (
	"time"
	"strings"
	"github.com/go-redis/redis"
	"github.com/areller/turing"
)

type redisAdapter struct {
	client *redis.Client
	keyGateway func (string) (string, bool)
}

func (ra *redisAdapter) convertError(err error) error {
	if err == nil {
		return nil
	}

	if err == redis.Nil {
		return turing.KeyNotExistsError
	} else if strings.Contains(err.Error(), "WRONGTYPE") {
		return turing.WrongTypeError
	} else if strings.Contains(err.Error(), "ERR") {
		return turing.GeneralError
	} else if err != nil {
		return turing.ConnectionDroppedError
	}

	return nil
}

func (ra *redisAdapter) filterKeys(keys []string) []string {
	var newKeys []string
	for _, k := range keys {
		nKey, ex := ra.keyGateway(k)
		if ex {
			newKeys = append(newKeys, nKey)
		}
	}
	return newKeys
}

func (ra *redisAdapter) Set(key string, value interface{}) error {
	key, ex := ra.keyGateway(key)
	if !ex {
		return turing.KeyNotExistsError
	}

	_, err := ra.client.Set(key, value, -1).Result()
	return ra.convertError(err)
}

func (ra *redisAdapter) Get(key string) (string, error) {
	key, ex := ra.keyGateway(key)
	if !ex {
		return "", turing.KeyNotExistsError
	}

	res, err := ra.client.Get(key).Result()
	err = ra.convertError(err)
	return res, err
}

func (ra *redisAdapter) Delete(keys ...string) (int, error) {
	keys = ra.filterKeys(keys)
	res, err := ra.client.Del(keys...).Result()
	err = ra.convertError(err)
	return int(res), err
}

func (ra *redisAdapter) Exists(keys ...string) (int, error) {
	keys = ra.filterKeys(keys)
	res, err := ra.client.Exists(keys...).Result()
	err = ra.convertError(err)
	return int(res), err
}

func (ra *redisAdapter) HSet(key string, field string, value interface{}) error {
	key, ex := ra.keyGateway(key)
	if !ex {
		return turing.KeyNotExistsError
	}

	_, err := ra.client.HSet(key, field, value).Result()
	return ra.convertError(err)
}

func (ra *redisAdapter) HSetMany(key string, kv map[string]interface{}) error {
	key, ex := ra.keyGateway(key)
	if !ex {
		return turing.KeyNotExistsError
	}

	_, err := ra.client.HMSet(key, kv).Result()
	return ra.convertError(err)
}

func (ra *redisAdapter) HGet(key string, field string) (string, error) {
	key, ex := ra.keyGateway(key)
	if !ex {
		return "", turing.KeyNotExistsError
	}

	res, err := ra.client.HGet(key, field).Result()
	err = ra.convertError(err)
	return res, err
}

func (ra *redisAdapter) HGetAll(key string) (map[string]string, error) {
	key, ex := ra.keyGateway(key)
	if !ex {
		return nil, turing.KeyNotExistsError
	}

	res, err := ra.client.HGetAll(key).Result()
	err = ra.convertError(err)
	return res, err
}

func (ra *redisAdapter) HDelete(key string, fields ...string) (int, error) {
	key, ex := ra.keyGateway(key)
	if !ex {
		return 0, turing.KeyNotExistsError
	}

	res, err := ra.client.HDel(key, fields...).Result()
	err = ra.convertError(err)
	return int(res), err
}

func (ra *redisAdapter) Expire(key string, expiry time.Duration) error {
	key, ex := ra.keyGateway(key)
	if !ex {
		return turing.KeyNotExistsError
	}

	_, err := ra.client.Expire(key, expiry).Result()
	return ra.convertError(err)
}

func AdaptToKVStore(client *redis.Client, keyGateway func (key string) (string, bool)) *redisAdapter {
	if keyGateway == nil {
		keyGateway = func (key string) (string, bool) {
			return key, true
		}
	}

	return &redisAdapter{
		client: client,
		keyGateway: keyGateway,
	}
}