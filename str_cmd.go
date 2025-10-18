package routeredis

import (
	"errors"

	"github.com/gomodule/redigo/redis"
	jsoniter "github.com/json-iterator/go"
)

func Get(key *Key, data any) (string, bool, error) {
	res, err := redis.String(DoCmdWithTTL(nil, "GET", key))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return "", false, nil
		}
		return "", false, err
	}
	return res, true, nil
}

func GetObj(key *Key, data any) (bool, error) {
	res, err := redis.String(DoCmdWithTTL(nil, "GET", key))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return false, nil
		}
		return false, err
	}
	err = jsoniter.UnmarshalFromString(res, data)
	if err != nil {
		return false, err
	}
	return true, err
}

func GetInt64(key *Key) (int64, bool, error) {
	res, err := redis.Int64(DoCmdWithTTL(nil, "GET", key))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return res, true, nil
}

func GetFloat64(key *Key) (float64, error) {
	return redis.Float64(DoCmdWithTTL(nil, "GET", key))
}

func Del(key *Key) error {
	_, err := DoCmdWithTTL(nil, "DEL", key)
	return err
}

func AsyncDel(key *Key) error {
	return SendCmdWithTTL(nil, "DEL", key)
}

func Setex(key *Key, ttl int64, data any) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	_, err := DoCmdWithTTL(nil, "SETEX", key, ttl, str)
	if err != nil {
		return err
	}

	return nil
}

func AsyncSetex(key *Key, ttl int64, data any) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}
	return SendCmdWithTTL(nil, "SETEX", key, ttl, str)
}

func Set(key *Key, data any, ttl int64) error {
	if ttl > 0 {
		return Setex(key, ttl, data)
	}

	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	if _, err := DoCmdWithTTL(nil, "SET", key, str); err != nil {
		return err
	}

	return nil
}

func AsyncSet(key *Key, data any, ttl int64) error {
	if ttl > 0 {
		return AsyncSetex(key, ttl, data)
	}

	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	if err := SendCmdWithTTL(nil, "SET", key, str); err != nil {
		return err
	}

	return nil
}

func Incrby(key *Key, inc int64, ttl int64) (int64, error) {
	res, err := redis.Int64(DoCmdWithTTL(NewAsyncSecTTL(ttl), "INCRBY", key, inc))
	if err != nil {
		return 0, err
	}

	return res, nil
}

func AsyncIncrby(key *Key, inc int64, ttl int64) error {
	if err := SendCmdWithTTL(NewAsyncSecTTL(ttl), "INCRBY", key, inc); err != nil {
		return err
	}

	return nil
}

func Setnx(key *Key, data any, ttl int64) (bool, error) {
	res, err := redis.Int(DoCmdWithTTL(NewAsyncSecTTL(ttl), "SETNX", key, data))
	if err != nil {
		return false, err
	}
	return res == 1, nil
}
