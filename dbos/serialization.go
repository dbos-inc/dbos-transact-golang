package dbos

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"reflect"
)

const (
	// nilMarker is a special marker string used to represent nil values in the database.
	nilMarker = "__DBOS_NIL"
)

// Serializer defines the interface for encoding and decoding workflow data for storage.
// The type parameter T determines what types the serializer handles.
// The built-in JSON serializer uses concrete types (Serializer[P]) for correct struct unmarshaling.
// Custom serializers implement Serializer[any] and must embed type info in payloads (e.g., using a type envelope)
type Serializer[T any] interface {
	// Name returns the name of the serialization format (e.g., "DBOS_JSON", "DBOS_GOB").
	Name() string
	// Encode serializes a value to a string representation for database storage.
	Encode(data T) (*string, error)
	// Decode deserializes a string from the database back into a value.
	Decode(data *string) (T, error)
}

type jsonSerializer[T any] struct{}

func newJSONSerializer[T any]() Serializer[T] {
	return &jsonSerializer[T]{}
}

func (j *jsonSerializer[T]) Name() string {
	return "DBOS_JSON"
}

func (j *jsonSerializer[T]) Encode(data T) (*string, error) {
	// Check if the value is nil (for pointer types, slice, map, etc.)
	if isNilValue(data) {
		// For nil values, return the special marker so it can be stored in the database
		marker := string(nilMarker)
		return &marker, nil
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
	if data == nil || *data == nilMarker {
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

// GobSerializer implements Serializer[any] using Go's gob encoding.
// Users must call gob.Register(ConcreteType{}) for each concrete type
// used in workflow inputs, outputs, events, and messages.
type GobSerializer struct{}

// NewGobSerializer returns a new gob-based serializer.
func NewGobSerializer() Serializer[any] {
	return &GobSerializer{}
}

func (g *GobSerializer) Name() string {
	return "DBOS_GOB"
}

func (g *GobSerializer) Encode(data any) (*string, error) {
	if isNilValue(data) {
		marker := string(nilMarker)
		return &marker, nil
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&data); err != nil {
		return nil, fmt.Errorf("failed to gob encode data: %w", err)
	}
	encodedStr := base64.StdEncoding.EncodeToString(buf.Bytes())
	return &encodedStr, nil
}

func (g *GobSerializer) Decode(data *string) (any, error) {
	if data == nil || *data == nilMarker {
		return nil, nil
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 data: %w", err)
	}

	var result any
	dec := gob.NewDecoder(bytes.NewReader(decodedBytes))
	if err := dec.Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to gob decode data: %w", err)
	}

	return result, nil
}

// encodeValue uses the custom serializer if set, otherwise the default JSON serializer.
func encodeValue(customSer Serializer[any], data any) (*string, error) {
	if customSer != nil {
		return customSer.Encode(data)
	}
	return newJSONSerializer[any]().Encode(data)
}

// decodeValue uses the custom serializer if set, otherwise the typed JSON serializer.
// The typed JSON serializer is essential because json.Unmarshal needs the concrete type
// to produce a struct (not map[string]interface{}).
func decodeValue[T any](customSer Serializer[any], data *string) (T, error) {
	if customSer != nil {
		decoded, err := customSer.Decode(data)
		if err != nil {
			return *new(T), err
		}
		if decoded == nil {
			return getNilOrZeroValue[T](), nil
		}
		typed, ok := decoded.(T)
		if !ok {
			return *new(T), fmt.Errorf("custom serializer returned %T, expected %T", decoded, *new(T))
		}
		return typed, nil
	}
	return newJSONSerializer[T]().Decode(data)
}

// getCustomSerializer extracts the custom serializer from a DBOSContext, if set.
func getCustomSerializer(ctx DBOSContext) Serializer[any] {
	if dc, ok := ctx.(*dbosContext); ok {
		return dc.serializer
	}
	return nil
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
