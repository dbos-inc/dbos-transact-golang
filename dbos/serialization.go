package dbos

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
)

// Serializer defines the interface for pluggable serializers.
// Encode and Decode are called during database storage and retrieval respectively.
type Serializer interface {
	// Encode serializes data to a string for database storage
	Encode(data any) (string, error)
	// Decode deserializes data from a string
	Decode(data *string) (any, error)
}

// GobSerializer implements Serializer using encoding/gob
type GobSerializer struct{}

func NewGobSerializer() *GobSerializer {
	return &GobSerializer{}
}

// Encode serializes data using gob and encodes it to a base64 string
// Performs "lazy" registration of the type with gob
func (g *GobSerializer) Encode(data any) (string, error) {
	var inputBytes []byte
	if !isNilValue(data) {
		// Register the type with gob for proper serialization
		safeGobRegister(data, nil)

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(&data); err != nil {
			return "", fmt.Errorf("failed to encode data: %w", err)
		}
		inputBytes = buf.Bytes()
	}
	return base64.StdEncoding.EncodeToString(inputBytes), nil
}

// Decode deserializes data from a base64 string using gob
func (g *GobSerializer) Decode(data *string) (any, error) {
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

// JSONSerializer implements Serializer using encoding/json
type JSONSerializer struct{}

func NewJSONSerializer() *JSONSerializer {
	return &JSONSerializer{}
}

func (j *JSONSerializer) Encode(data any) (string, error) {
	var inputBytes []byte
	if !isNilValue(data) {
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return "", fmt.Errorf("failed to marshal data to JSON: %w", err)
		}
		inputBytes = jsonBytes
	}
	return base64.StdEncoding.EncodeToString(inputBytes), nil
}

func (j *JSONSerializer) Decode(data *string) (any, error) {
	if data == nil || *data == "" {
		return nil, nil
	}

	dataBytes, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode data: %w", err)
	}

	var result any
	if err := json.Unmarshal(dataBytes, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON data: %w", err)
	}

	return result, nil
}

// serialize serializes data using the serializer from the DBOSContext
// this is only use in workflow handles
func serialize(ctx DBOSContext, data any) (string, error) {
	dbosCtx, ok := ctx.(*dbosContext)
	if !ok {
		return "", fmt.Errorf("invalid DBOSContext: expected *dbosContext")
	}
	if dbosCtx.serializer == nil {
		return "", fmt.Errorf("no serializer configured in DBOSContext")
	}
	return dbosCtx.serializer.Encode(data)
}

// Handle cases where the provided data interface wraps a nil value (e.g., var p *int; data := any(p). data != nil but the underlying value is nil)
func isNilValue(data any) bool {
	if data == nil {
		return true
	}
	v := reflect.ValueOf(data)
	// Check if the value is invalid (zero Value from reflect)
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
		return v.IsNil()
	}
	return false
}

// safeGobRegister attempts to register a type with gob, recovering only from
// panics caused by duplicate type/name registrations (e.g., registering both T and *T).
// These specific conflicts don't affect encoding/decoding correctness, so they're safe to ignore.
// Other panics (like register `any`) are real errors and will propagate.
func safeGobRegister(value any, logger *slog.Logger) {
	defer func() {
		if r := recover(); r != nil {
			if errStr, ok := r.(string); ok {
				// Check if this is one of the two specific duplicate registration errors we want to ignore
				// See https://cs.opensource.google/go/go/+/refs/tags/go1.25.1:src/encoding/gob/type.go;l=832
				if strings.Contains(errStr, "gob: registering duplicate types for") ||
					strings.Contains(errStr, "gob: registering duplicate names for") {
					if logger != nil {
						logger.Debug("gob registration conflict", "type", fmt.Sprintf("%T", value), "error", r)
					}
					return
				}
			}
			// Re-panic for any other errors
			panic(r)
		}
	}()
	gob.Register(value)
}
