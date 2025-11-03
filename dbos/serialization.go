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
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal data to JSON: %w", err)
	}
	inputBytes = jsonBytes
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
		// We cannot just use the serializer's Decode method and recast -- the type information would be lost
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

	// Handle pointer types: if T is a pointer type and decoded value matches the element type,
	// convert the value to a pointer
	// Get the type of T by reflecting on zero (or creating a new value if zero is nil)
	tType := reflect.TypeOf(zero)
	if tType == nil {
		// zero is nil, T is likely a pointer type - get type from a new value
		var tVal T
		tType = reflect.TypeOf(&tVal).Elem()
	}

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
