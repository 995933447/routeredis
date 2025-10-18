package routeredis

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

type OnCmdDoneFunc func(execCmdWay string, ttl *TTL, cost time.Duration, cmd string, key *Key, args ...any)

var OnCmdDone OnCmdDoneFunc

type TTL struct {
	IsAsyncTTL    bool
	TTL           int64
	IsMillisecond bool
}

func NewTTL(isAsyncTTL bool, ttl int64, isMillisecond bool) *TTL {
	return &TTL{
		IsAsyncTTL:    isAsyncTTL,
		TTL:           ttl,
		IsMillisecond: isMillisecond,
	}
}

func NewSyncSecTTL(ttl int64) *TTL {
	return &TTL{
		IsAsyncTTL:    false,
		TTL:           ttl,
		IsMillisecond: false,
	}
}

func NewAsyncSecTTL(ttl int64) *TTL {
	return &TTL{
		IsAsyncTTL:    true,
		TTL:           ttl,
		IsMillisecond: false,
	}
}

func NewSecTTL(isAsyncTTL bool, ttl int64) *TTL {
	return NewTTL(isAsyncTTL, ttl, false)
}

func NewAsyncMilliSecTTL(ttl int64) *TTL {
	return &TTL{
		IsAsyncTTL:    true,
		TTL:           ttl,
		IsMillisecond: false,
	}
}

func NewSyncMilliSecTTL(isAsyncTTL bool, ttl int64) *TTL {
	return NewTTL(isAsyncTTL, ttl, false)
}

func NewMilliSecTTL(isAsyncTTL bool, ttl int64) *TTL {
	return NewTTL(isAsyncTTL, ttl, true)
}

func DoCmdWithTTL(ttl *TTL, cmd string, key *Key, args ...any) (res any, err error) {
	conn, err := RouteConn(key.Route)
	if err != nil {
		return nil, err
	}

	if OnCmdDone != nil {
		start := time.Now()
		defer OnCmdDone("DoCmdWithTTL", ttl, time.Since(start), cmd, key, args...)
	}

	if err = conn.Err(); err != nil {
		return 0, err
	}

	res, err = doCmdWithTTL(conn, ttl, cmd, key.Key, args...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func doCmdWithTTL(conn redis.Conn, ttl *TTL, cmd string, key string, args ...any) (any, error) {
	defer conn.Close()
	
	reply, err := conn.Do(cmd, append([]any{key}, args...)...)
	if err == nil {
		if ttl != nil && ttl.TTL > 0 {
			setTTLCmd := "EXPIRE"
			if ttl.IsMillisecond {
				setTTLCmd = "PEXPIRE"
			}
			if !ttl.IsAsyncTTL {
				_, _ = conn.Do(setTTLCmd, key, ttl.TTL)
			} else {
				if _, ok := conn.(*ClusterConn); ok {
					_, _ = conn.Do(setTTLCmd, key, ttl.TTL)
				} else {
					_ = conn.Send(setTTLCmd, key, ttl.TTL)
				}
			}
		}
	}

	return reply, err
}

func SendCmdWithTTL(ttl *TTL, cmd string, key *Key, args ...any) error {
	conn, err := RouteConn(key.Route)
	if err != nil {
		return err
	}

	if OnCmdDone != nil {
		start := time.Now()
		defer OnCmdDone("SendCmdWithTTL", ttl, time.Since(start), cmd, key, args...)
	}

	if err := conn.Err(); err != nil {
		return err
	}

	if _, ok := conn.(*ClusterConn); ok {
		_, err = doCmdWithTTL(conn, ttl, cmd, key.Key, args...)
		if err != nil {
			return err
		}
	}

	defer conn.Close()

	err = conn.Send(cmd, append([]any{key.Key}, args...)...)
	if err == nil {
		if ttl != nil && ttl.TTL > 0 {
			setTTLCmd := "EXPIRE"
			if ttl.IsMillisecond {
				setTTLCmd = "PEXPIRE"
			}
			_ = conn.Send(setTTLCmd, key.Key, ttl.TTL)
		}
	}

	return err
}
