package routeredis

import (
	"errors"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

type ConnConf struct {
	IdleCount              int // 最大空闲连接数
	IdleTimeoutMillSec     int
	MaxConnLifetimeMillSec int
	MaxConnPoolSize        int      // 最大链接数量
	Servers                []string // 非分片集群模式下只能配置一个server
	Password               string
	EnabledCluster         bool
}

var (
	ErrRedisConnPoolNotRegistered = errors.New("redis conn pool not registered")
	ErrRedisKeyRouteNotRegistered = errors.New("redis key route not registered")
	ErrNoServerAvailable          = errors.New("no server is available")
)

const DefaultConnName = "default"

type RedisPool interface {
	Get() redis.Conn
	Close() error
}

var _ RedisPool = (*RedisCluster)(nil)

type RedisCluster struct {
	*redisc.Cluster
}

type ClusterConn struct {
	redis.Conn
}

func (r *RedisCluster) Get() redis.Conn {
	conn, _ := redisc.RetryConn(r.Cluster.Get(), 10, 10*time.Millisecond)
	return &ClusterConn{conn}
}

var redisPools sync.Map

func ConnectByConf(connName string, conf *ConnConf) error {
	if conf.EnabledCluster {
		return ConnectClusterByConf(connName, conf)
	}

	pool := &redis.Pool{
		MaxIdle:         conf.IdleCount,
		MaxActive:       conf.MaxConnPoolSize, //when zero,there's no limit. https://godoc.org/github.com/garyburd/redigo/redis#Pool
		IdleTimeout:     time.Duration(conf.IdleTimeoutMillSec) * time.Millisecond,
		MaxConnLifetime: time.Duration(conf.MaxConnLifetimeMillSec) * time.Millisecond,
		Wait:            true,
		Dial: func() (redis.Conn, error) {
			if len(conf.Servers) == 0 {
				return nil, ErrNoServerAvailable
			}

			server := conf.Servers[0]
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}

			if conf.Password != "" {
				if _, err := c.Do("AUTH", conf.Password); err != nil {
					c.Close()
					return nil, err
				}
			}

			return c, nil
		},
		TestOnBorrow: redisOnBorrow,
	}

	Connect(connName, pool)

	return nil
}

func ConnectDefaultByConf(conf *ConnConf) error {
	return ConnectByConf(DefaultConnName, conf)
}

func ConnectClusterByConf(connName string, conf *ConnConf) error {
	cluster := &redisc.Cluster{
		StartupNodes: conf.Servers,
		DialOptions: []redis.DialOption{
			redis.DialPassword(conf.Password),
		},
		CreatePool: func(addr string, opts ...redis.DialOption) (*redis.Pool, error) {
			pool := &redis.Pool{
				MaxIdle:         conf.IdleCount,
				MaxActive:       conf.MaxConnPoolSize, //when zero,there's no limit. https://godoc.org/github.com/garyburd/redigo/redis#Pool
				IdleTimeout:     time.Duration(conf.IdleTimeoutMillSec) * time.Millisecond,
				MaxConnLifetime: time.Duration(conf.MaxConnLifetimeMillSec) * time.Millisecond,
				Wait:            true,
				TestOnBorrow:    redisOnBorrow,
			}
			pool.Dial = func() (redis.Conn, error) {
				conn, err := redis.Dial("tcp", addr, opts...)
				if err != nil {
					return nil, err
				}

				if conf.Password != "" {
					if _, err := conn.Do("AUTH", conf.Password); err != nil {
						conn.Close()
						return nil, err
					}
				}

				return conn, nil
			}
			return pool, nil
		},
	}

	return ConnectCluster(connName, cluster)
}

func ConnectDefaultClusterByConf(conf *ConnConf) error {
	return ConnectClusterByConf(DefaultConnName, conf)
}

func redisOnBorrow(c redis.Conn, t time.Time) error {
	if time.Since(t) < time.Minute {
		return nil
	}
	_, err := c.Do("PING")
	return err
}

func Connect(connName string, redisPool RedisPool) {
	oldPoolAny, ok := redisPools.Load(connName)
	if ok {
		go func() {
			time.Sleep(time.Second * 5)
			_ = oldPoolAny.(RedisPool).Close()
		}()
	}
	redisPools.Store(connName, redisPool)
}

func ConnectDefault(redisPool RedisPool) {
	Connect(DefaultConnName, redisPool)
}

func ConnectCluster(connName string, redisCluster *redisc.Cluster) error {
	if err := redisCluster.Refresh(); err != nil {
		return err
	}

	Connect(connName, &RedisCluster{redisCluster})

	return nil
}

func ConnectDefaultCluster(redisCluster *redisc.Cluster) error {
	return ConnectCluster(DefaultConnName, redisCluster)
}

func GetConnPool(connName string) (RedisPool, error) {
	redisPool, ok := redisPools.Load(connName)
	if !ok {
		return nil, ErrRedisConnPoolNotRegistered
	}
	return redisPool.(RedisPool), nil
}

func GetConn(connName string) (redis.Conn, error) {
	pool, err := GetConnPool(connName)
	if err != nil {
		return nil, err
	}
	return pool.Get(), nil
}
