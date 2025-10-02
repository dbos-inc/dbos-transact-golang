package dbos

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"log/slog"
	"strings"
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

// safeGobRegister attempts to register a type with gob, recovering only from
// panics caused by duplicate type/name registrations (e.g., registering both T and *T).
// These specific conflicts don't affect encoding/decoding correctness, so they're safe to ignore.
// Other panics (like register `any`) are real errors and will propagate.
func safeGobRegister(value any, logger *slog.Logger) {
	defer func() {
		if r := recover(); r != nil {
			// Only catch specific duplicate registration errors
			if errStr, ok := r.(string); ok {
				// Check if this is one of the two specific duplicate registration errors we want to ignore
				// These patterns come from gob's RegisterName function
				// See https://cs.opensource.google/go/go/+/refs/tags/go1.25.1:src/encoding/gob/type.go;l=832
				if strings.Contains(errStr, "gob: registering duplicate types for") ||
					strings.Contains(errStr, "gob: registering duplicate names for") {
					// Log at debug level since these specific conflicts are expected and harmless
					if logger != nil {
						logger.Debug("gob registration conflict",
							"type", fmt.Sprintf("%T", value),
							"error", r)
					}
					return // Recover from this specific panic
				}
			}
			// Re-panic for any other errors
			panic(r)
		}
	}()
	gob.Register(value)
}
