package routeredis

import (
	"fmt"
	"sync"

	"github.com/gomodule/redigo/redis"
)

func NewKey(route string, tmpl string, args ...any) *Key {
	return &Key{
		Route: route,
		Key:   fmt.Sprintf(tmpl, args...),
	}
}

type Key struct {
	Route string
	Key   string
}

var keyRoutes sync.Map

func RegisterKeyRoute(route string, connName string) {
	keyRoutes.Store(route, connName)
}

func RegisterDefaultConnKeyRoute(route string) {
	RegisterKeyRoute(route, DefaultConnName)
}

func RouteConnPool(route string) (RedisPool, error) {
	connName, ok := keyRoutes.Load(route)
	if !ok {
		return nil, ErrRedisKeyRouteNotRegistered
	}
	return GetConnPool(connName.(string))
}

func RouteConn(route string) (redis.Conn, error) {
	pool, err := RouteConnPool(route)
	if err != nil {
		return nil, err
	}
	return pool.Get(), nil
}
