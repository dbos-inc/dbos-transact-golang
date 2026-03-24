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

	// PortableSerializerName is the serialization format name for cross-language interop.
	PortableSerializerName = "portable_json"
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

// portableWorkflowArgs is the cross-language envelope for workflow inputs.
type portableWorkflowArgs struct {
	PositionalArgs []json.RawMessage `json:"positionalArgs"`
	NamedArgs      map[string]any    `json:"namedArgs"`
}

// encodePortableArgs wraps a single Go workflow input into the portable args envelope as plain JSON.
func encodePortableArgs(data any) (*string, error) {
	argBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal portable arg: %w", err)
	}
	envelope := portableWorkflowArgs{
		PositionalArgs: []json.RawMessage{argBytes},
		NamedArgs:      map[string]any{},
	}
	b, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal portable args envelope: %w", err)
	}
	s := string(b)
	return &s, nil
}

// decodePortableArgs unwraps the first positional arg from the portable args envelope into T.
func decodePortableArgs[T any](data *string) (T, error) {
	if data == nil || *data == "null" {
		return getNilOrZeroValue[T](), nil
	}
	var envelope portableWorkflowArgs
	if err := json.Unmarshal([]byte(*data), &envelope); err != nil {
		return *new(T), fmt.Errorf("failed to unmarshal portable args envelope: %w", err)
	}
	if len(envelope.PositionalArgs) == 0 {
		return getNilOrZeroValue[T](), nil
	}
	var result T
	if err := json.Unmarshal(envelope.PositionalArgs[0], &result); err != nil {
		return *new(T), fmt.Errorf("failed to unmarshal portable arg into %T: %w", *new(T), err)
	}
	return result, nil
}

// serializerName returns the name of the active serializer, defaulting to "DBOS_JSON" if nil.
func serializerName(ser Serializer[any]) string {
	if ser != nil {
		return ser.Name()
	}
	return "DBOS_JSON"
}

// encodeValue uses the custom serializer if set, otherwise the default JSON serializer.
func encodeValue(customSer Serializer[any], data any) (*string, error) {
	if customSer != nil {
		return customSer.Encode(data)
	}
	return newJSONSerializer[any]().Encode(data)
}

// decodeValue decodes a value from the database using the serialization format stored alongside it.
// Dispatch logic:
//  1. If customSer matches storedSerialization (and isn't portable) → use customSer
//  2. If storedSerialization is "portable_json" → use PortableJSONSerializer + re-marshal to T
//  3. If storedSerialization is "DBOS_JSON" or empty → use the built-in typed JSON serializer
//  4. Otherwise → unknown format error
func decodeValue[T any](customSer Serializer[any], data *string, storedSerialization string) (T, error) {
	if customSer != nil && customSer.Name() == storedSerialization && storedSerialization != PortableSerializerName {
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
	if storedSerialization == PortableSerializerName {
		// Portable workflow inputs are wrapped in {"positionalArgs": [...], "namedArgs": {}}.
		// Unwrap the first positional arg and decode into T.
		return decodePortableArgs[T](data)
	}
	if storedSerialization == "" || storedSerialization == "DBOS_JSON" {
		return newJSONSerializer[T]().Decode(data)
	}
	return *new(T), fmt.Errorf("unknown serialization format %q", storedSerialization)
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
