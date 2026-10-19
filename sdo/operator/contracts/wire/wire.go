// Package wire is the one place SDO encodes and decodes contract messages.
// Every message that crosses a seam (task, result, outcome, ledger, state,
// telemetry) is ProtoJSON with proto field names, and every encode and decode
// runs the protovalidate rules the .proto declares, so a malformed message
// fails at the boundary that produced or received it.
package wire

import (
	"encoding/json"
	"fmt"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func name(message proto.Message) protoreflect.FullName {
	return message.ProtoReflect().Descriptor().FullName()
}

// Validate checks message against its protovalidate rules.
func Validate(message proto.Message) error {
	if err := protovalidate.Validate(message); err != nil {
		return fmt.Errorf("invalid %s: %w", name(message), err)
	}
	return nil
}

// Marshal validates message and encodes it as ProtoJSON with proto field
// names. Zero-valued fields are omitted.
func Marshal(message proto.Message) ([]byte, error) {
	if err := Validate(message); err != nil {
		return nil, err
	}
	return protojson.MarshalOptions{UseProtoNames: true}.Marshal(message)
}

// MarshalFull is Marshal with zero-valued fields emitted, for documents whose
// durable form names every field.
func MarshalFull(message proto.Message) ([]byte, error) {
	if err := Validate(message); err != nil {
		return nil, err
	}
	return protojson.MarshalOptions{UseProtoNames: true, EmitDefaultValues: true}.Marshal(message)
}

// Canonical is Marshal with sorted keys and no insignificant whitespace.
// ProtoJSON does not promise stable output, so byte comparison uses this form.
func Canonical(message proto.Message) ([]byte, error) {
	raw, err := Marshal(message)
	if err != nil {
		return nil, err
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// Unmarshal strictly decodes ProtoJSON into message (an unknown field is an
// error) and validates the result.
func Unmarshal(data []byte, message proto.Message) error {
	if err := protojson.Unmarshal(data, message); err != nil {
		return fmt.Errorf("decode %s: %w", name(message), err)
	}
	return Validate(message)
}

// UnmarshalLenient is Unmarshal for documents written by older builds: unknown
// fields are dropped. The result is still validated.
func UnmarshalLenient(data []byte, message proto.Message) error {
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, message); err != nil {
		return fmt.Errorf("decode %s: %w", name(message), err)
	}
	return Validate(message)
}

// DecodeLenientUnvalidated drops unknown fields like UnmarshalLenient but does
// not validate. It exists for the one caller that must migrate a retired
// representation into a valid message before validating it (legacy outcome
// import); every other decoder uses Unmarshal or UnmarshalLenient.
func DecodeLenientUnvalidated(data []byte, message proto.Message) error {
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, message); err != nil {
		return fmt.Errorf("decode %s: %w", name(message), err)
	}
	return nil
}

// Digest is a deterministic binary encoding of message, for content digests.
func Digest(message proto.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}
