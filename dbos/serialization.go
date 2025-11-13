package dbos

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"reflect"
	"strings"
)

type serializer[T any] interface {
	Encode(data T) (string, error)
	Decode(data *string) (T, error)
}

// gobValue is a wrapper type for gob encoding/decoding of any value
// It prevents encoding nil values directly, and helps us differentiate nil values and empty strings
type gobValue[T any] struct {
	Value T
}

// safeGobRegister attempts to register a type with gob, recovering only from
// panics caused by duplicate type/name registrations (e.g., registering both T and *T).
// These specific conflicts don't affect encoding/decoding correctness, so they aren't errors.
// Other panics (like registering `any`) are real errors and will propagate.
func safeGobRegister(value any) {
	defer func() {
		if r := recover(); r != nil {
			if errStr, ok := r.(string); ok {
				// Check if this is one of the two specific duplicate registration errors we want to ignore
				// See https://cs.opensource.google/go/go/+/refs/tags/go1.25.1:src/encoding/gob/type.go;l=832
				if strings.Contains(errStr, "gob: registering duplicate types for") ||
					strings.Contains(errStr, "gob: registering duplicate names for") {
					return
				}
			}
			// Re-panic for any other errors
			panic(r)
		}
	}()
	gob.Register(value)
}

// init registers the gobValue wrapper type with gob for gobSerializer
func init() {
	// Register wrapper type - this is required for gob encoding/decoding to work
	safeGobRegister(gobValue[any]{})
}

type gobSerializer[T any] struct{}

func newGobSerializer[T any]() serializer[T] {
	return &gobSerializer[T]{}
}

func (g *gobSerializer[T]) Encode(data T) (string, error) {
	if isNilValue(data) {
		// For nil values, encode an empty byte slice directly to base64
		return base64.StdEncoding.EncodeToString([]byte{}), nil
	}

	// Register the type before encoding
	safeGobRegister(data)

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	wrapper := gobValue[T]{Value: data}
	if err := encoder.Encode(wrapper); err != nil {
		return "", fmt.Errorf("failed to encode data: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

func (g *gobSerializer[T]) Decode(data *string) (T, error) {
	var zero T

	if data == nil || *data == "" {
		return zero, nil
	}

	dataBytes, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return zero, fmt.Errorf("failed to decode base64 data: %w", err)
	}

	// If decoded data is empty, it represents a nil value
	if len(dataBytes) == 0 {
		return zero, nil
	}

	safeGobRegister(zero)

	var wrapper gobValue[T]
	decoder := gob.NewDecoder(bytes.NewReader(dataBytes))
	if err := decoder.Decode(&wrapper); err != nil {
		return zero, fmt.Errorf("failed to decode gob data: %w", err)
	}

	return wrapper.Value, nil
}

// isNilValue checks if a value is nil (for pointer types, slice, map, etc.)
func isNilValue(v any) bool {
	val := reflect.ValueOf(v)
	if !val.IsValid() {
		return true
	}
	switch val.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return val.IsNil()
	}
	return false
}

// IsNestedPointer checks if a type is a nested pointer (e.g., **int, ***int).
// It returns false for non-pointer types and single-level pointers (*int).
// It returns true for nested pointers with depth > 1.
func IsNestedPointer(t reflect.Type) bool {
	if t == nil {
		return false
	}

	depth := 0
	currentType := t

	// Count pointer indirection levels, break early if depth > 1
	for currentType != nil && currentType.Kind() == reflect.Pointer {
		depth++
		if depth > 1 {
			return true
		}
		currentType = currentType.Elem()
	}

	return false
}
