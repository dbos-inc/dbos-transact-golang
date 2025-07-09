package dbos

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
)

func serialize(data any) (string, error) {
	var inputBytes []byte
	if data != nil {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(&data); err != nil {
			return "", fmt.Errorf("failed to encode data: %w", err)
		}
		inputBytes = buf.Bytes()
	}
	return base64.StdEncoding.EncodeToString(inputBytes), nil
}

func deserialize(data *string) (any, error) {
	if data == nil || *data == "" {
		return nil, nil
	}

	dataBytes, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode data: %w", err)
	}

	var result any
	buf := bytes.NewBuffer(dataBytes)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode data: %w", err)
	}

	return result, nil
}
