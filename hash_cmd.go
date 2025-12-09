package routeredis

import (
	"errors"

	"github.com/gomodule/redigo/redis"
	jsoniter "github.com/json-iterator/go"
)

func Hget(key *Key, field any) (string, bool, error) {
	res, err := redis.String(DoCmdWithTTL(nil, "HGET", key, field))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return "", false, nil
		}
		return "", false, err
	}
	return res, true, nil
}

func HgetInt64(key *Key, field any) (int64, bool, error) {
	res, err := redis.Int64(DoCmdWithTTL(nil, "HGET", key, field))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return res, true, nil
}

func HgetFloat64(key *Key, field any) (float64, bool, error) {
	res, err := redis.Float64(DoCmdWithTTL(nil, "HGET", key, field))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return res, true, nil
}

func Hgetall(key *Key) (map[string]string, bool, error) {
	m := make(map[string]string)
	data, err := redis.Strings(DoCmdWithTTL(nil, "HGETALL", key))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if len(data)%2 != 0 {
		return m, true, nil
	}

	dataLen := len(data)

	for i := 0; i < dataLen; i += 2 {
		k := data[i]
		m[k] = data[i+1]
	}
	return m, true, nil
}

func HgetallStrings(key *Key) ([]string, bool, error) {
	res, err := redis.Strings(DoCmdWithTTL(nil, "HGETALL", key))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return []string{}, false, nil
		}
		return nil, false, err
	}
	return res, true, nil
}

func Hsetnx(key *Key, field, data any, ttl int64) (bool, error) {
	str, ok := data.(string)
	if !ok { //不是字符串
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return false, err
		}
	}

	res, err := redis.Int(DoCmdWithTTL(NewAsyncSecTTL(ttl), "HSETNX", key, field, str))
	if err != nil {
		return false, err
	}

	return res == 1, nil
}

func Hset(key *Key, field, data any, ttl int64) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	_, err := DoCmdWithTTL(NewAsyncSecTTL(ttl), "HSET", key, field, str)
	if err != nil {
		return err
	}

	return nil
}

func AsyncHset(key *Key, field, data any, ttl int64) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}
	err := SendCmdWithTTL(NewAsyncSecTTL(ttl), "HSET", key, field, str)
	if err != nil {
		return nil
	}

	return nil
}

func Hmset(ttl int64, key *Key, data ...any) error {
	if _, err := DoCmdWithTTL(NewAsyncSecTTL(ttl), "HMSET", key, data...); err != nil {
		return err
	}
	return nil
}

func AsyncHmset(ttl int64, key *Key, data ...any) error {
	err := SendCmdWithTTL(NewAsyncSecTTL(ttl), "HMSET", key, data...)
	if err != nil {
		return err
	}

	return nil
}

func Hmget(key *Key, fields ...any) ([]int64, bool, error) {
	res, err := redis.Int64s(DoCmdWithTTL(nil, "HMGET", key, fields...))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return nil, false, nil
		}
		return nil, false, err
	}

	return res, true, nil
}

func Hmdel(key *Key, fields ...any) error {
	_, err := DoCmdWithTTL(nil, "HDEL", key, fields...)
	if err != nil {
		return err
	}
	return nil
}

func HgetallMapUint64ToInt64(key *Key) (map[uint64]int64, bool, error) {
	m := map[uint64]int64{}

	data, err := redis.Int64s(DoCmdWithTTL(nil, "HGETALL", key))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return nil, false, nil
		}
		return nil, false, err
	}

	if len(data) != 0 {
		return m, true, nil
	}

	dataLen := len(data)

	for i := 0; i < dataLen; i += 2 {
		k := uint64(data[i])
		m[k] = data[i+1]
	}
	return m, true, nil
}

func HgetallInt64Map(key *Key) (map[int64]int64, bool, error) {
	m := map[int64]int64{}

	data, err := redis.Int64s(DoCmdWithTTL(nil, "HGETALL", key))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return nil, false, nil
		}
		return nil, false, err
	}

	if len(data)%2 != 0 {
		return m, true, nil
	}

	dataLen := len(data)
	for i := 0; i < dataLen; i += 2 {
		k := data[i]
		m[k] = data[i+1]
	}
	return m, true, nil
}

func Hincrby(key *Key, field any, inc int64, ttl int64) (int64, error) {
	return redis.Int64(DoCmdWithTTL(NewAsyncSecTTL(ttl), "HINCRBY", key, field, inc))
}

func AsyncHincrby(key *Key, field any, inc int64, ttl int64) error {
	return SendCmdWithTTL(NewAsyncSecTTL(ttl), "HINCRBY", key, field, inc)
}

func Hdel(key *Key, field any) error {
	_, err := DoCmdWithTTL(nil, "HDEL", key, field)
	if err != nil {
		return err
	}
	return nil
}

func AsyncHdel(key *Key, field any) error {
	return SendCmdWithTTL(nil, "HDEL", key, field)
}

func Hexists(key *Key, field any) (bool, error) {
	res, err := redis.Bool(DoCmdWithTTL(nil, "HEXISTS", key, field))
	if err != nil {
		return false, err
	}
	return res, nil
}
