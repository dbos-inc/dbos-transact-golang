package dbos

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
)

// Serializer defines the interface for pluggable serializers.
// Encode and Decode are called during database storage and retrieval respectively.
type Serializer interface {
	// Encode serializes data to a string for database storage
	Encode(data any) (string, error)
	// Decode deserializes data from a string
	Decode(data *string) (any, error)
}

// JSONSerializer implements Serializer using encoding/json
type JSONSerializer struct{}

func NewJSONSerializer() *JSONSerializer {
	return &JSONSerializer{}
}

func isJSONSerializer(s Serializer) bool {
	_, ok := s.(*JSONSerializer)
	return ok
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

// serialize serializes data using the provided serializer
func serialize[T any](serializer Serializer, data T) (string, error) {
	if serializer == nil {
		return "", fmt.Errorf("serializer cannot be nil")
	}
	return serializer.Encode(data)
}

// deserialize decodes an encoded string directly into a typed variable.
// For JSON serializer, this decodes directly into the target type, preserving type information.
// For other serializers, it decodes into any and then type-asserts.
// (we don't want generic Serializer interface because it would require 1 serializer per type)
func deserialize[T any](serializer Serializer, encoded *string) (T, error) {
	if serializer == nil {
		return *new(T), fmt.Errorf("serializer cannot be nil")
	}

	var zero T
	if encoded == nil || *encoded == "" {
		return zero, nil
	}

	if isJSONSerializer(serializer) {
		// For JSON serializer, decode directly into the target type to preserve type information
		// We cannot just use the serializer's Decode method and recast -- the type inormation would be lost
		dataBytes, err := base64.StdEncoding.DecodeString(*encoded)
		if err != nil {
			return zero, fmt.Errorf("failed to decode base64 data: %w", err)
		}

		// We could check and error explicitly if T is an interface type.

		if err := json.Unmarshal(dataBytes, &zero); err != nil {
			return zero, fmt.Errorf("failed to unmarshal JSON data: %w", err)
		}
		return zero, nil
	}

	// For other serializers, just call the decoder and type-assert
	decoded, err := serializer.Decode(encoded)
	if err != nil {
		return zero, err
	}
	typedResult, ok := decoded.(T)
	if !ok {
		return zero, fmt.Errorf("cannot convert decoded value of type %T to %T", decoded, zero)
	}
	return typedResult, nil
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
