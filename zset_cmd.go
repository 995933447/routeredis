package routeredis

import (
	"errors"
	"strconv"

	"github.com/gomodule/redigo/redis"
	jsoniter "github.com/json-iterator/go"
)

func Zcard(key *Key) (int64, error) {
	return redis.Int64(DoCmdWithTTL(nil, "ZCARD", key))
}

func ZscanWithScore(key *Key, cursor, count int64) (int64, map[string]int64, error) {
	mapKeyToScore := map[string]int64{}

	res, err := redis.Values(DoCmdWithTTL(nil, "ZSCAN", key, cursor, "COUNT", count))
	if err != nil {
		return 0, nil, err
	}

	cursor, err = redis.Int64(res[0], nil)
	if err != nil {
		return 0, nil, err
	}

	keyWithScores, err := redis.Strings(res[1], nil)
	if err != nil {
		return 0, nil, err
	}

	var (
		k     string
		score int64
	)
	size := len(keyWithScores)
	for i := 1; i <= size; i++ {
		keyOrScore := keyWithScores[i-1]
		if i%2 == 0 {
			score, err = strconv.ParseInt(keyOrScore, 10, 64)
			if err != nil {
				return 0, nil, err
			}
			mapKeyToScore[k] = score
			score = 0
			k = ""
			continue
		}

		k = keyOrScore
	}
	return cursor, mapKeyToScore, nil
}

func ZscanWithoutScore(key *Key, cursor, count int64) (int64, []string, error) {
	res, err := redis.Values(DoCmdWithTTL(nil, "ZSCAN", key, cursor, "COUNT", count))
	if err != nil {
		return 0, nil, err
	}

	cursor, err = redis.Int64(res[0], nil)
	if err != nil {
		return 0, nil, err
	}

	keyWithScores, err := redis.Strings(res[1], nil)
	if err != nil {
		return 0, nil, err
	}

	var keys []string
	size := len(keyWithScores)
	for i := 1; i <= size; i++ {
		if i%2 == 0 {
			continue
		}
		keys = append(keys, keyWithScores[i-1])
	}

	return cursor, keys, nil
}

func Zadd(key *Key, score int64, data any, ttl int64) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	_, err := DoCmdWithTTL(NewAsyncSecTTL(ttl), "ZADD", key, score, str)
	if err != nil {
		return err
	}

	return err
}

func AsyncZadd(key *Key, score int64, data any, ttl int64) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	if _, err := DoCmdWithTTL(NewAsyncSecTTL(ttl), "ZADD", key, score, str); err != nil {
		return err
	}

	return nil
}

func ZaddMany(ttl int64, key *Key, data ...any) error {
	if _, err := DoCmdWithTTL(NewAsyncSecTTL(ttl), "ZADD", key, data...); err != nil {
		return err
	}

	return nil
}

func Zscore(key *Key, data any) (int64, error) {
	return redis.Int64(DoCmdWithTTL(nil, "ZSCORE", key, data))
}

func Zincrby(key *Key, inc int64, data any, ttl int64) error {
	_, err := DoCmdWithTTL(NewAsyncSecTTL(ttl), "ZINCRBY", key, inc, data)
	if err != nil {
		return err
	}

	return nil
}

func AsyncZincrby(key *Key, inc int64, data any, ttl int64) error {
	return SendCmdWithTTL(NewAsyncSecTTL(ttl), "ZINCRBY", key, inc, data)
}

func Zcount(key *Key, start any, end any) (int64, error) {
	return redis.Int64(DoCmdWithTTL(nil, "ZCOUNT", key, start, end))
}

func Zrevrangebyscore(key *Key, start, end any, withScore bool) ([]string, error) {
	var args []any
	args = append(args, end)
	args = append(args, start)
	if withScore {
		args = append(args, "WITHSCORES")
	}
	return redis.Strings(DoCmdWithTTL(nil, "ZREVRANGEBYSCORE", key, args...))
}

func ZrevrangebyscoreInt64s(key *Key, start, end any, withScore bool) ([]int64, error) {
	var args []any
	args = append(args, key)
	args = append(args, end)
	args = append(args, start)
	if withScore {
		args = append(args, "WITHSCORES")
	}
	return redis.Int64s(DoCmdWithTTL(nil, "ZREVRANGEBYSCORE", key, args...))
}

func Zrangerbyscore(key *Key, start, end any, withScore bool) ([]string, error) {
	var args []any
	args = append(args, start)
	args = append(args, end)
	if withScore {
		args = append(args, "WITHSCORES")
	}
	return redis.Strings(DoCmdWithTTL(nil, "ZRANGEBYSCORE", key, args...))
}

func ZrangerbyscoreInt64s(key *Key, start any, end any, withScore bool) ([]int64, error) {
	var args []any
	args = append(args, start)
	args = append(args, end)
	if withScore {
		args = append(args, "WITHSCORES")
	}
	return redis.Int64s(DoCmdWithTTL(nil, "ZRANGEBYSCORE", key, args...))
}

func Zremrangebyrank(key *Key, start, end any) error {
	_, err := DoCmdWithTTL(nil, "ZREMRANGEBYRANK", key, start, end)
	if err != nil {
		return err
	}

	return nil
}

func AsyncZremrangebyscore(key *Key, start, end any) error {
	return SendCmdWithTTL(nil, "ZREMRANGEBYSCORE", key, start, end)
}

func Zrange(key *Key, start, end any, withScore bool) ([]string, error) {
	var args []any
	args = append(args, start)
	args = append(args, end)
	if withScore {
		args = append(args, "WITHSCORES")
	}

	return redis.Strings(DoCmdWithTTL(nil, "ZRANGE", key, args...))
}

func ZrangeInt64s(key *Key, start, end any, withScore bool) ([]int64, error) {
	var args []any
	args = append(args, start)
	args = append(args, end)
	if withScore {
		args = append(args, "WITHSCORES")
	}

	return redis.Int64s(DoCmdWithTTL(nil, "ZRANGE", key, args...))
}

func Zrevrange(key *Key, start, end any, withScore bool) ([]string, error) {
	var args []any
	args = append(args, start)
	args = append(args, end)
	if withScore {
		args = append(args, "WITHSCORES")
	}

	return redis.Strings(DoCmdWithTTL(nil, "ZREVRANGE", key, args...))
}

func ZrevrangeInt64s(key *Key, start, end any, withScore bool) ([]int64, error) {
	var args []any
	args = append(args, start)
	args = append(args, end)
	if withScore {
		args = append(args, "WITHSCORES")
	}

	return redis.Int64s(DoCmdWithTTL(nil, "ZREVRANGE", key, args...))
}

func Zrevrank(key *Key, data any) (int64, error) {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return 0, err
		}
	}

	return redis.Int64(DoCmdWithTTL(nil, "ZREVRANK", key, str))
}

func AsyncZrem(key *Key, data any) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}
	return SendCmdWithTTL(nil, "ZREM", key, str)
}

func Zexists(key *Key, member string) (bool, error) {
	_, err := redis.Int64(DoCmdWithTTL(nil, "ZSCORE", key, member))
	if err != nil {
		if !errors.Is(err, redis.ErrNil) {
			return false, err
		}

		return false, nil
	}
	return true, nil
}

func Zunionstore(key1, key2 *Key) (int64, error) {
	count, err := redis.Int64(DoCmdWithTTL(nil, "ZUNIONSTORE", key2, 1, key1.Key))
	if err != nil {
		if !errors.Is(err, redis.ErrNil) {
			return 0, err
		}
		return 0, nil
	}

	return count, nil
}
