package dbos

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
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

// PortableWorkflowArgs is the cross-language envelope for workflow inputs.
// Use this to pass positional and/or named arguments when enqueuing
// a workflow that will be executed by a DBOS application in another language.
//
// Example:
//
//	args := dbos.PortableWorkflowArgs{
//	    PositionalArgs: []any{"hello", 42},
//	    NamedArgs:      map[string]any{"key": "value"},
//	}
//	handle, err := client.Enqueue("queue", "pyWorkflow", args)
type PortableWorkflowArgs struct {
	PositionalArgs []any          `json:"positionalArgs"`
	NamedArgs      map[string]any `json:"namedArgs"`
}

// portableArgsRaw is used internally for decoding, where json.RawMessage
// preserves the original JSON for type-safe unmarshaling of individual args.
type portableArgsRaw struct {
	PositionalArgs []json.RawMessage `json:"positionalArgs"`
	NamedArgs      map[string]any    `json:"namedArgs"`
}

// encodePortableArgs wraps a value into the portable args envelope and encodes it as plain JSON.
// If the value is already a PortableWorkflowArgs, it is encoded as-is.
// Otherwise, the value is placed as the single positional arg inside a new envelope.
func encodePortableArgs(data any) (*string, error) {
	var toEncode any
	if _, ok := data.(PortableWorkflowArgs); ok {
		toEncode = data
	} else {
		argBytes, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal portable arg: %w", err)
		}
		toEncode = portableArgsRaw{
			PositionalArgs: []json.RawMessage{argBytes},
			NamedArgs:      map[string]any{},
		}
	}
	return newPortableSerializer[any]().Encode(toEncode)
}

// decodePortableArgs unwraps the first positional arg from the portable args envelope into T.
// If T is PortableWorkflowArgs, the full envelope is decoded as-is (no unwrapping).
func decodePortableArgs[T any](data *string) (T, error) {
	if data == nil || *data == "null" {
		return getNilOrZeroValue[T](), nil
	}
	// If T is the envelope type itself, decode the full data directly.
	if reflect.TypeFor[T]() == reflect.TypeFor[PortableWorkflowArgs]() {
		var result T
		if err := json.Unmarshal([]byte(*data), &result); err != nil {
			return *new(T), fmt.Errorf("failed to decode portable args envelope as %T: %w", *new(T), err)
		}
		return result, nil
	}
	var envelope portableArgsRaw
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

// PortableWorkflowError is the cross-language error type for workflows using portable serialization.
// When a workflow using the portable JSON format fails, errors are stored in this structure,
// readable by all DBOS-supported languages.
//
// Raise a PortableWorkflowError to pass structured error info to callers in other languages:
//
//	return nil, &dbos.PortableWorkflowError{Name: "ValidationError", Message: "invalid input", Code: 400}
type PortableWorkflowError struct {
	Name    string `json:"name"`           // Error type/class name
	Message string `json:"message"`        // Human-readable error message
	Code    any    `json:"code,omitempty"` // Optional application-specific error code (number or string)
	Data    any    `json:"data,omitempty"` // Optional structured error details
}

func (e *PortableWorkflowError) Error() string {
	return e.Message
}

// serializeWorkflowError serializes an error for DB storage.
// For portable workflows, uses the portable JSON format ({"name":..., "message":..., ...}).
// For all others, stores the plain error string.
func serializeWorkflowError(err error, serialization string) string {
	if serialization != PortableSerializerName {
		return err.Error()
	}
	var errData PortableWorkflowError
	if pe := (*PortableWorkflowError)(nil); errors.As(err, &pe) {
		errData = *pe
	} else {
		errData = PortableWorkflowError{
			Name:    "Portable Error",
			Message: err.Error(),
		}
	}
	b, jsonErr := json.Marshal(errData)
	if jsonErr != nil {
		return err.Error() // fallback to plain string
	}
	return string(b)
}

// deserializeWorkflowError deserializes an error from DB storage.
// For portable serialization, parses the JSON into a PortableWorkflowError.
// For all others, creates a plain error from the string.
func deserializeWorkflowError(errStr *string, serialization string) error {
	if errStr == nil || *errStr == "" {
		return nil
	}
	if serialization != PortableSerializerName {
		return errors.New(*errStr)
	}
	var pe PortableWorkflowError
	if err := json.Unmarshal([]byte(*errStr), &pe); err != nil {
		return errors.New(*errStr) // fallback: return plain error
	}
	return &pe
}
