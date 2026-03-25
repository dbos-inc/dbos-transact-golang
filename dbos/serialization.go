package dbos

import (
	"bytes"
	"context"
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

type jsonSerializer[T any] struct {
	portable bool
}

func newJSONSerializer[T any]() Serializer[T] {
	return &jsonSerializer[T]{portable: false}
}

func newPortableSerializer[T any]() Serializer[T] {
	return &jsonSerializer[T]{portable: true}
}

func (j *jsonSerializer[T]) Name() string {
	if j.portable {
		return PortableSerializerName
	}
	return "DBOS_JSON"
}

func (j *jsonSerializer[T]) Encode(data T) (*string, error) {
	if isNilValue(data) {
		if j.portable {
			s := "null"
			return &s, nil
		}
		marker := string(nilMarker)
		return &marker, nil
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode data: %w", err)
	}

	if j.portable {
		s := string(jsonBytes)
		return &s, nil
	}
	encodedStr := base64.StdEncoding.EncodeToString(jsonBytes)
	return &encodedStr, nil
}

func (j *jsonSerializer[T]) Decode(data *string) (T, error) {
	if j.portable {
		if data == nil || *data == "null" {
			return getNilOrZeroValue[T](), nil
		}
		var result T
		if err := json.Unmarshal([]byte(*data), &result); err != nil {
			return result, fmt.Errorf("failed to decode portable json data: %w", err)
		}
		return result, nil
	}

	if data == nil || *data == nilMarker {
		return getNilOrZeroValue[T](), nil
	}

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

// typedCustomSerializerAdapter wraps a user-provided Serializer[any] into Serializer[T],
// handling the type assertion on decode.
type typedCustomSerializerAdapter[T any] struct {
	inner Serializer[any]
}

func (a *typedCustomSerializerAdapter[T]) Name() string {
	return a.inner.Name()
}

func (a *typedCustomSerializerAdapter[T]) Encode(data T) (*string, error) {
	return a.inner.Encode(data)
}

func (a *typedCustomSerializerAdapter[T]) Decode(data *string) (T, error) {
	decoded, err := a.inner.Decode(data)
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

// resolveEncoder returns the serializer to use for encoding values within a workflow.
// Priority: portable workflow → user custom serializer → default JSON.
func resolveEncoder(ctx context.Context) Serializer[any] {
	if wfState, ok := ctx.Value(workflowStateKey).(*workflowState); ok && wfState != nil && wfState.isPortableWorkflow {
		return newPortableSerializer[any]()
	}
	if dc, ok := ctx.(*dbosContext); ok && dc.serializer != nil {
		return dc.serializer
	}
	return newJSONSerializer[any]()
}

// resolveDecoder returns a typed serializer for decoding a value based on the stored serialization format.
// Priority: portable_json → user custom serializer → default JSON.
func resolveDecoder[T any](storedSerialization string, customSer Serializer[any]) (Serializer[T], error) {
	if storedSerialization == PortableSerializerName {
		return newPortableSerializer[T](), nil
	}
	if customSer != nil && customSer.Name() == storedSerialization {
		return &typedCustomSerializerAdapter[T]{inner: customSer}, nil
	}
	if storedSerialization == "" || storedSerialization == "DBOS_JSON" {
		return newJSONSerializer[T](), nil
	}
	return nil, fmt.Errorf("unknown serialization format %q", storedSerialization)
}

// getCustomSerializerFromCtx extracts the user-provided custom serializer from a DBOSContext, if set.
func getCustomSerializerFromCtx(ctx DBOSContext) Serializer[any] {
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
