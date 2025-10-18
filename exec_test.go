package routeredis

import (
	"testing"

	"github.com/gomodule/redigo/redis"
)

func TestExec(t *testing.T) {
	InitRedis()

	key := getKey(123)

	_, err := DoCmdWithTTL(NewAsyncSecTTL(600), "SET", key, "barbarbar")
	if err != nil {
		t.Error(err)
	}

	t.Log(redis.String(DoCmdWithTTL(nil, "GET", key)))

	err = SendCmdWithTTL(NewAsyncSecTTL(50), "SET", key, "bar bar bar")
	if err != nil {
		t.Error(err)
	}

	t.Log(redis.String(DoCmdWithTTL(nil, "GET", key)))
}

const (
	ConnNameUser = "user"
	KeyRouteSess = "session"
)

func InitRedis() {
	ConnectByConf(ConnNameUser, &ConnConf{
		Servers:         []string{"127.0.0.1:6379"},
		MaxConnPoolSize: 10,
	})

	RegisterKeyRoute(KeyRouteSess, ConnNameUser)
}

func getKey(userId int64) *Key {
	return NewKey(KeyRouteSess, "username:%d", userId)
}
