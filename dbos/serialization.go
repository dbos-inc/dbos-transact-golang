package dbos

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"
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
//
// Every implementation must honor this contract:
//
//  1. Decode can be called with a nil *string, even though the serializer's own
//     Encode never produced one: some checkpoints record an error and never write
//     an output, so the stored value is SQL NULL. Decode must tolerate nil input
//     and choose its own nil semantics (typically returning the zero value of T).
//  2. Encode may be called with a nil or zero value, and the nil round-trip must
//     be lossless: Decode(Encode(nil-value)) must yield that nil value back.
//  3. The literal string "__DBOS_NIL" is reserved by the engine and wire-frozen.
//     A custom Encode must never emit it for non-nil data; a value stored as
//     "__DBOS_NIL" would be misreported as nil by observability paths.
//  4. Encode must not return a nil *string. To represent nil data, return a
//     pointer to a sentinel string (e.g. the built-in gob serializer stores
//     "__DBOS_NIL"; the portable JSON serializer stores "null").
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

// gobSerializer implements Serializer[any] using Go's gob encoding.
// Users must call gob.Register(ConcreteType{}) for each concrete type
// used in workflow inputs, outputs, events, and messages.
type gobSerializer struct{}

// NewGobSerializer returns a new gob-based serializer.
func NewGobSerializer() Serializer[any] {
	return &gobSerializer{}
}

func (g *gobSerializer) Name() string {
	return "DBOS_GOB"
}

func (g *gobSerializer) Encode(data any) (*string, error) {
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

func (g *gobSerializer) Decode(data *string) (any, error) {
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
//	handle, err := dbos.Enqueue[any](client, "queue", "pyWorkflow", args)
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

// decodeListingValue prepares a persisted value for listing/display paths
// (ListWorkflows, GetWorkflowSteps) based on the stored per-row format:
//   - portable rows decode into their generic JSON value
//   - rows matching the configured custom serializer decode with it
//   - default JSON rows return their raw JSON text (base64-decoded), leaving
//     any further decoding to the caller
//
// If the format is unknown or decoding fails, it returns the raw stored
// string alongside the error.
func decodeListingValue(encoded *string, storedSerialization string, customSer Serializer[any]) (any, error) {
	switch {
	case storedSerialization == PortableSerializerName:
		var decoded any
		if err := json.Unmarshal([]byte(*encoded), &decoded); err != nil {
			return *encoded, err
		}
		return decoded, nil
	case customSer != nil && customSer.Name() == storedSerialization:
		decoded, err := customSer.Decode(encoded)
		if err != nil {
			return *encoded, err
		}
		return decoded, nil
	case storedSerialization == "" || storedSerialization == "DBOS_JSON":
		decodedBytes, err := base64.StdEncoding.DecodeString(*encoded)
		if err != nil {
			return *encoded, err
		}
		return string(decodedBytes), nil
	default:
		return *encoded, fmt.Errorf("unknown serialization format %q", storedSerialization)
	}
}

// listingValueJSON renders a listing value as JSON text for wire protocols
// (conductor, admin server). Default JSON rows already carry their JSON text
// as a string and pass through unchanged; decoded values (portable or custom
// serializer rows) are marshaled.
func listingValueJSON(v any) (string, bool) {
	if s, ok := v.(string); ok && json.Valid([]byte(s)) {
		return s, true
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", false
	}
	return string(b), true
}

// getCustomSerializerFromCtx extracts the user-provided custom serializer, if set.
// It accepts any context but only real DBOS contexts (a Client or Context,
// both *dbosContext under the hood) carry one; mocks and plain contexts yield nil.
func getCustomSerializerFromCtx(ctx context.Context) Serializer[any] {
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

func init() {
	// Register the DBOS error types so they can be gob-encoded with their concrete
	// type preserved on the Go <-> Go error path (see serializeWorkflowError).
	// Error (formerly DBOSError) must keep its original wire name: stored errors
	// were encoded under it, and decode looks types up by name. Do not change this
	// pin — it is what keeps v0-persisted error payloads decodable.
	gob.RegisterName("*dbos.DBOSError", &Error{})
	gob.Register(&PortableWorkflowError{})
	gob.Register(time.Time{})
	// Register the scheduled workflow input so gob-serialized schedule firings
	// round-trip without users having to register it themselves.
	gob.Register(ScheduledWorkflowInput{})
}

// serializeWorkflowError encodes an error for DB storage.
//   - Portable workflows use the cross-language JSON envelope
//     ({"name":..., "message":..., ...}), readable by every DBOS language.
//   - All other (Go <-> Go) workflows gob-encode the error so its concrete Go type
//     (e.g. *Error) is preserved and reconstructed as-is on decode. Errors whose type
//     cannot be gob-encoded (e.g. errors.New/fmt.Errorf) fall back to their plain string.
func serializeWorkflowError(logger *slog.Logger, err error, serialization string) string {
	if err == nil {
		return ""
	}
	if serialization != PortableSerializerName {
		// Go <-> Go: gob-encode so the concrete error type (e.g. *Error) is preserved.
		// Types that cannot be gob-encoded (e.g. errors.New/fmt.Errorf) fall back to their string.
		if encoded, gobErr := NewGobSerializer().Encode(err); gobErr == nil && encoded != nil {
			return *encoded
		} else if logger != nil {
			logger.Warn("workflow error type cannot be gob-encoded; persisting its message only: errors.Is/errors.As will not match this error when read back from the database",
				"error_type", fmt.Sprintf("%T", err), "error", err, "encode_error", gobErr)
		}
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

// deserializeWorkflowError decodes an error stored by serializeWorkflowError. The stored
// format is self-describing, so no serialization hint is needed: gob-encoded (Go <-> Go)
// errors are decoded back into their original Go type (e.g. *Error), portable JSON
// envelopes into a *PortableWorkflowError, and anything else (a legacy or fallback plain
// string) into a normal error.
func deserializeWorkflowError(errStr *string) error {
	if errStr == nil || *errStr == "" {
		return nil
	}
	// Go <-> Go errors are gob-encoded; decode preserves their original type (e.g. *Error).
	// A portable JSON envelope or legacy plain string fails base64/gob and falls through below.
	if decoded, gobErr := NewGobSerializer().Decode(errStr); gobErr == nil {
		if e, ok := decoded.(error); ok {
			if de, isDBOS := e.(*Error); isDBOS {
				// wrappedErr is unexported and not gob-encoded; rebuild stdlib
				// causes (context.Canceled/DeadlineExceeded) from CauseKind.
				de.RestoreWrappedCause()
			}
			return e
		}
	}
	var pe PortableWorkflowError
	if err := json.Unmarshal([]byte(*errStr), &pe); err == nil && (pe.Name != "" || pe.Message != "") {
		return &pe
	}
	return &plainError{msg: *errStr}
}

// plainError carries a legacy or fallback stored error string. Unlike errors.New
// (whose message field is unexported and marshals as {}), it stays readable when
// surfaced in JSON, e.g. through WorkflowStatus.
type plainError struct{ msg string }

func (e *plainError) Error() string { return e.msg }

func (e *plainError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Message string `json:"message"`
	}{Message: e.msg})
}
