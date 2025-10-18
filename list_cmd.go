package routeredis

import (
	"github.com/gomodule/redigo/redis"
	jsoniter "github.com/json-iterator/go"
)

func Rpop(key *Key) (string, error) {
	res, err := redis.Strings(DoCmdWithTTL(nil, "BRPOP", key, 1))
	if err != nil {
		return "", err
	}
	if len(res) < 2 {
		return "", nil
	}
	return res[1], nil
}

func Llen(key *Key) (int64, error) {
	return redis.Int64(DoCmdWithTTL(nil, "LLEN", key))
}

func AsyncLpush(key *Key, data any, ttl int64) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	err := SendCmdWithTTL(NewAsyncSecTTL(ttl), "LPUSH", key, str)
	if err != nil {
		return err
	}

	return nil
}

func Lpush(key *Key, data any, ttl int64) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	_, err := DoCmdWithTTL(NewAsyncSecTTL(ttl), "LPUSH", key, str)
	if err != nil {
		return err
	}

	return nil
}

func Rpush(key *Key, data any, ttl int64) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	_, err := DoCmdWithTTL(NewAsyncSecTTL(ttl), "RPUSH", key, str)
	if err != nil {
		return err
	}

	return nil
}

func AsyncRpush(key *Key, data any, ttl int64) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	err := SendCmdWithTTL(NewAsyncSecTTL(ttl), "RPUSH", key, str)
	if err != nil {
		return err
	}

	return nil
}

func Lpop(key *Key) (string, error) {
	res, err := redis.Strings(DoCmdWithTTL(nil, "BLPOP", key, 1))
	if err != nil {
		return "", err
	}

	if len(res) < 2 {
		return "", nil
	}

	return res[1], nil
}

func AsyncLrem(key *Key, data any) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	err := SendCmdWithTTL(nil, "LREM", key, 0, str)
	if err != nil {
		return err
	}

	return nil
}

func Lrange(key *Key, start, end int32) ([]string, error) {
	return redis.Strings(DoCmdWithTTL(nil, "LRANGE", key, start, end))
}

func LrangeInt64(key *Key, start, end int32) ([]int64, error) {
	return redis.Int64s(DoCmdWithTTL(nil, "LRANGE", key, start, end))
}

func Ltrim(key *Key, start, end int32) error {
	_, err := DoCmdWithTTL(nil, "LTRIM", key, start, end)
	if err != nil {
		return err
	}

	return nil
}
