package operability

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Encode returns the canonical JSON of a valid report: field order is fixed by
// the types, there are no maps and no timestamps, so equal state encodes to equal
// bytes.
func (r Report) Encode() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(r.clone())
}

// Digest is the hex SHA-256 of the canonical JSON.
func (r Report) Digest() (string, error) {
	b, err := r.Encode()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// Decode parses a report strictly: unknown fields, trailing data, an unknown
// schema and any structural violation are errors.
func Decode(data []byte) (Report, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var r Report
	if err := dec.Decode(&r); err != nil {
		return Report{}, fmt.Errorf("decode operability report: %w", err)
	}
	if dec.More() {
		return Report{}, fmt.Errorf("decode operability report: trailing data")
	}
	if err := r.Validate(); err != nil {
		return Report{}, err
	}
	return r, nil
}
