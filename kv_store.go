package turing

import (
	"time"
)

type KVStore interface {
	Set(key string, value interface{}) error
	Get(key string) (string, error)
	Delete(keys ...string) (int, error)
	Exists(key ...string) (int, error)
	HSet(key string, field string, value interface{}) error
	HSetMany(key string, kv map[string]interface{}) error
	HGet(key string, field string) (string, error)
	HGetAll(key string) (map[string]string, error)
	HDelete(key string, fields ...string) (int, error)
	Expire(key string, expiry time.Duration) error
}