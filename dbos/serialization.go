package dbos

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"reflect"
	"strings"
)

// serializer defines the interface for pluggable serializers.
// Encode and Decode are called during database storage and retrieval respectively.
type serializer interface {
	// Encode serializes data to a string for database storage
	Encode(data any) (string, error)
	// Decode deserializes data from a string
	Decode(data *string) (any, error)
}

func isGobSerializer(s serializer) bool {
	_, ok := s.(*gobSerializer)
	return ok
}

// gobValue is a wrapper type for gob encoding/decoding of any value
// Useful when we need to encode/decode a nil pointer or an empty string
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

// gobSerializer implements serializer using encoding/gob
type gobSerializer struct{}

func newGobSerializer() *gobSerializer {
	return &gobSerializer{}
}

func (g *gobSerializer) Encode(data any) (string, error) {
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
		return "", fmt.Errorf("failed to encode data with gob: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

func (g *gobSerializer) Decode(data *string) (any, error) {
	if data == nil || *data == "" {
		return nil, nil
	}

	dataBytes, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 data: %w", err)
	}

	// If decoded data is empty, it represents a nil value
	if len(dataBytes) == 0 {
		return nil, nil
	}

	var wrapper gobValue
	decoder := gob.NewDecoder(bytes.NewReader(dataBytes))
	if err := decoder.Decode(&wrapper); err != nil {
		return nil, fmt.Errorf("failed to decode gob data: %w", err)
	}

	return wrapper.Value, nil
}

// deserialize decodes an encoded string directly into a typed variable.
func deserialize[T any](serializer serializer, encoded *string) (T, error) {
	zero := *new(T)

	if serializer == nil {
		return zero, fmt.Errorf("serializer cannot be nil")
	}

	if encoded == nil || *encoded == "" {
		return zero, nil
	}

	// Get the type of T once at the beginning of the function
	tType := reflect.TypeOf(zero)
	if tType == nil {
		// zero is nil, T is likely a pointer type or interface
		// Get the type from a pointer to zero
		tType = reflect.TypeOf(&zero).Elem()
	}

	// For gobSerializer, register type T before decoding
	// This is required on the recovery path, where the process might not have been doing the encode/registering.
	if isGobSerializer(serializer) {
		// Check if T is an interface type - if so, skip registration
		// We do no support interface types in workflows/steps
		// Only register if T is a concrete type (not an interface)
		if tType != nil && tType.Kind() != reflect.Interface {
			safeGobRegister(zero)
		}
	}

	// For other serializers, just call the decoder and type-assert
	decoded, err := serializer.Decode(encoded)
	if err != nil {
		return zero, err
	}

	// Handle pointer types: if T is a pointer type and decoded value matches the element type,
	// convert the value to a pointer

	if tType != nil && tType.Kind() == reflect.Pointer {
		// T is a pointer type
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

	// Try direct type assertion
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
