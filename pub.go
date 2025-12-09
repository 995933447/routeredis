package routeredis

import (
	jsoniter "github.com/json-iterator/go"
)

func Publish(key *Key, data any) error {
	str, ok := data.(string)
	if !ok {
		var err error
		str, err = jsoniter.MarshalToString(data)
		if err != nil {
			return err
		}
	}

	_, err := DoCmdWithTTL(nil, "PUBLISH", key, str)
	if err != nil {
		return err
	}

	return nil
}
