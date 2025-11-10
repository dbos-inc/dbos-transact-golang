package dbos

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
)

type serializer[T any] interface {
	Encode(data T) (*string, error)
	Decode(data *string) (T, error)
}

type jsonSerializer[T any] struct{}

func newJSONSerializer[T any]() serializer[T] {
	return &jsonSerializer[T]{}
}

func (j *jsonSerializer[T]) Encode(data T) (*string, error) {
	// Check if the value is nil (for pointer types, slice, map, etc.)
	if isNilValue(data) {
		// For nil values, return nil pointer
		return nil, nil
	}

	// Check if the value is a zero value (but not nil)
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode data: %w", err)
	}
	encodedStr := base64.StdEncoding.EncodeToString(jsonBytes)
	return &encodedStr, nil
}

func (j *jsonSerializer[T]) Decode(data *string) (T, error) {
	// If data is a nil pointer, return nil (for pointer types) or zero value (for non-pointer types)
	if data == nil {
		return getNilOrZeroValue[T](), nil
	}

	// If *data is an empty string, return zero value
	var result T
	dataBytes, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return result, fmt.Errorf("failed to decode base64 data: %w", err)
	}

	if err := json.Unmarshal(dataBytes, &result); err != nil {
		return result, fmt.Errorf("failed to decode json data: %w", err)
	}

	return result, nil
}

// isNilValue checks if a value is nil (for pointer types, slice, map, etc.).
func isNilValue(v any) bool {
	val := reflect.ValueOf(v)
	if !val.IsValid() {
		return true
	}
	switch val.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func, reflect.Interface:
		return val.IsNil()
	}
	return false
}

// getNilOrZeroValue returns nil for pointer types, or zero value for non-pointer types.
func getNilOrZeroValue[T any]() T {
	var result T
	resultType := reflect.TypeOf(result)
	if resultType == nil {
		return result
	}
	// If T is a pointer type, return nil
	if resultType.Kind() == reflect.Pointer {
		return reflect.Zero(resultType).Interface().(T)
	}
	// Otherwise return zero value
	return result
}
