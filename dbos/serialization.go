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

// init registers the gobValue wrapper type with gob for gobSerializer
func init() {
	// Register wrapper type - this is required for gob encoding/decoding to work
	safeGobRegister(gobValue{})
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
	wrapper := gobValue{Value: data}
	if err := encoder.Encode(wrapper); err != nil {
		return "", fmt.Errorf("failed to encode data: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

func (g *gobSerializer[T]) Decode(data *string) (T, error) {
	zero := *new(T)

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

	// Resolve the type of T
	tType := reflect.TypeOf(zero)
	if tType == nil {
		// zero is nil, T is likely a pointer type or interface
		// Get the type from a pointer to T's zero value
		tType = reflect.TypeOf(&zero).Elem()
	}

	// Register type T before decoding
	// This is required on the recovery path, where the process might not have been doing the encode/registering.
	// This will panic if T is an non-registered interface type (which is not supported)
	if tType != nil && tType.Kind() != reflect.Interface {
		safeGobRegister(zero)
	}

	var wrapper gobValue
	decoder := gob.NewDecoder(bytes.NewReader(dataBytes))
	if err := decoder.Decode(&wrapper); err != nil {
		return zero, fmt.Errorf("failed to decode gob data: %w", err)
	}

	decoded := wrapper.Value

	// Gob stores pointed values directly, so we need to reconstruct the pointer type
	if tType != nil && tType.Kind() == reflect.Pointer {
		elemType := tType.Elem()
		decodedType := reflect.TypeOf(decoded)

		// Check if decoded value matches the element type (not the pointer type)
		if decodedType != nil && decodedType == elemType {
			// Create a new pointer to the decoded value
			elemValue := reflect.New(elemType)
			elemValue.Elem().Set(reflect.ValueOf(decoded))
			return elemValue.Interface().(T), nil
		}
		// If decoded is already a pointer of the correct type, try direct assertion
		if decodedType != nil && decodedType == tType {
			typedResult, ok := decoded.(T)
			if ok {
				return typedResult, nil
			}
		}
		// If decoded is nil and T is a pointer type, return nil pointer
		if decoded == nil {
			return zero, nil
		}
	}

	// Not a pointer -- direct type assertion
	typedResult, ok := decoded.(T)
	if !ok {
		return zero, fmt.Errorf("cannot convert decoded value of type %T to %T", decoded, zero)
	}
	return typedResult, nil
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
