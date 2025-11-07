package dbos

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
)

type serializer[T any] interface {
	Encode(data T) (string, error)
	Decode(data *string) (T, error)
}

type jsonSerializer[T any] struct{}

func newJSONSerializer[T any]() serializer[T] {
	return &jsonSerializer[T]{}
}

func (j *jsonSerializer[T]) Encode(data T) (string, error) {
	if isNilValue(data) {
		// For nil values, encode an empty byte slice directly to base64
		return base64.StdEncoding.EncodeToString([]byte{}), nil
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to encode data: %w", err)
	}
	return base64.StdEncoding.EncodeToString(jsonBytes), nil
}

func (j *jsonSerializer[T]) Decode(data *string) (T, error) {
	var result T

	if data == nil || *data == "" {
		return result, nil
	}

	dataBytes, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return result, fmt.Errorf("failed to decode base64 data: %w", err)
	}

	// If decoded data is empty, it represents a nil value
	if len(dataBytes) == 0 {
		return result, nil
	}

	// JSON unmarshal can handle both pointer and non-pointer types
	// For pointer types, it will allocate if needed
	if err := json.Unmarshal(dataBytes, &result); err != nil {
		return result, fmt.Errorf("failed to decode json data: %w", err)
	}

	return result, nil
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
