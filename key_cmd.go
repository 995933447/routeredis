package routeredis

import "github.com/gomodule/redigo/redis"

func Exists(key *Key) (bool, error) {
	return redis.Bool(DoCmdWithTTL(nil, "EXISTS", key))
}

func Type(key *Key) (string, error) {
	return redis.String(DoCmdWithTTL(nil, "TYPE", key))
}
