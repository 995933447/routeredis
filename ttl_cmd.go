package routeredis

func AsyncExpire(key *Key, ttl int64) error {
	return SendCmdWithTTL(nil, "EXPIRE", key, ttl)
}

func Expire(key *Key, ttl int64) error {
	_, err := DoCmdWithTTL(nil, "EXPIRE", key, ttl)
	if err != nil {
		return err
	}

	return nil
}
