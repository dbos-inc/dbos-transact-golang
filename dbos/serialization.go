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
type gobValue struct {
	Value any
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
	safeGobRegister(gobValue{})
}

type gobSerializer[T any] struct{}

func newGobSerializer[T any]() serializer[T] {
	return &gobSerializer[T]{}
}

func (g *gobSerializer[T]) Encode(data T) (string, error) {
	// Check if data is nil (for pointer types, slice, map, interface, chan, func)
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
	// Note wee do not support interface types in workflows/steps
	// This will panic if T is an non-registered interface type
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
