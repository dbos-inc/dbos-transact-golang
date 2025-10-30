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

func isJSONSerializer(s Serializer) bool {
	_, ok := s.(*JSONSerializer)
	return ok
}

// convertJSONToType converts a JSON-decoded value (map[string]interface{}) to type T
// via marshal/unmarshal round-trip.
//
// This is needed because JSON deserialization loses type information when decoding
// into `any` - it converts structs to map[string]interface{}, numbers to float64, etc.
// By re-marshaling and unmarshaling into a typed target, we (mostly) restore the original structure.
// We should be able to get ride of this when we lift encoding/decoding outside of the system database.
func convertJSONToType[T any](value any) (T, error) {
	if value == nil {
		return *new(T), nil
	}

	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return *new(T), fmt.Errorf("marshaling for type conversion: %w", err)
	}

	// Check if T is an interface type
	var zero T
	typeOfT := reflect.TypeOf(&zero).Elem()

	if typeOfT.Kind() == reflect.Interface {
		// T is interface - need to get concrete type from value
		concreteType := reflect.TypeOf(value)
		if concreteType.Kind() == reflect.Pointer {
			concreteType = concreteType.Elem()
		}

		// Create new instance of concrete type
		newInstance := reflect.New(concreteType)

		// Unmarshal into the concrete type
		if err := json.Unmarshal(jsonBytes, newInstance.Interface()); err != nil {
			return *new(T), fmt.Errorf("unmarshaling for type conversion: %w", err)
		}

		// Convert to interface type T
		return newInstance.Elem().Interface().(T), nil
	}

	var typedResult T
	if err := json.Unmarshal(jsonBytes, &typedResult); err != nil {
		return *new(T), fmt.Errorf("unmarshaling for type conversion: %w", err)
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
