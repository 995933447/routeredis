package routeredis

import (
	"github.com/gomodule/redigo/redis"
	jsoniter "github.com/json-iterator/go"
)

func AsyncSadd(key *Key, data any, ttl int64) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	_, err := DoCmdWithTTL(NewAsyncSecTTL(ttl), "SADD", key, str)
	if err != nil {
		return err
	}

	return nil
}

func Sismember(key *Key, value any) (bool, error) {
	res, err := redis.Int64(DoCmdWithTTL(nil, "SISMEMBER", key, value))
	if err != nil {
		return false, err
	}
	return res > 0, nil
}

func Scard(key *Key) (int, error) {
	return redis.Int(DoCmdWithTTL(nil, "SCARD", key))
}

func SpopInt64(key *Key) (int64, error) {
	return redis.Int64(DoCmdWithTTL(nil, "SPOP", key, 1))
}

func Spop(key *Key) (string, error) {
	return redis.String(DoCmdWithTTL(nil, "SPOP", key, 1))
}

func SmembersInt64s(key *Key) ([]int64, error) {
	return redis.Int64s(DoCmdWithTTL(nil, "SMEMBERS", key))
}

func Smembers(key *Key) ([]string, error) {
	return redis.Strings(DoCmdWithTTL(nil, "SMEMBERS", key))
}

func Srem(key *Key, value any) (bool, error) {
	res, err := redis.Int64(DoCmdWithTTL(nil, "SREM", key, value))
	if err != nil {
		return false, err
	}
	return res > 0, nil
}

func Sscan(key *Key, cursor, count int64) (int64, []string, error) {
	res, err := redis.Values(DoCmdWithTTL(nil, "SSCAN", key, cursor, "COUNT", count))
	if err != nil {
		return 0, nil, err
	}

	cursor, err = redis.Int64(res[0], nil)
	if err != nil {
		return 0, nil, err
	}

	keys, err := redis.Strings(res[1], nil)
	if err != nil {
		return 0, nil, err
	}

	return cursor, keys, nil
}
