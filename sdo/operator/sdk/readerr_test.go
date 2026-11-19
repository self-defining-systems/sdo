package sdk

import (
	"errors"
	"fmt"
	"testing"
)

func TestReadErrorClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil has no class", nil, ""},
		{"unclassified error is unavailable", errors.New("boom"), ReadClassUnavailable},
		{"refused", fmt.Errorf("login: %w", ErrRefused), ReadClassRefused},
		{"invalid", fmt.Errorf("secret: %w", ErrInvalidRead), ReadClassInvalid},
		{"unavailable", fmt.Errorf("no client: %w", ErrUnavailable), ReadClassUnavailable},
		{"object not captured is unavailable", fmt.Errorf("%w: v1/ConfigMap a/b", ErrObjectNotCaptured), ReadClassUnavailable},
		{"invalid outranks refused", errors.Join(ErrRefused, ErrInvalidRead), ReadClassInvalid},
		{"unavailable outranks refused", errors.Join(ErrUnavailable, ErrRefused), ReadClassUnavailable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ReadErrorClass(c.err); got != c.want {
				t.Fatalf("ReadErrorClass(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

func TestObjectNotCapturedIsUnavailable(t *testing.T) {
	if !errors.Is(ErrObjectNotCaptured, ErrUnavailable) {
		t.Fatal("ErrObjectNotCaptured must wrap ErrUnavailable")
	}
}

func TestErrorWithClassRebuildsSentinel(t *testing.T) {
	for _, class := range []string{ReadClassRefused, ReadClassUnavailable, ReadClassInvalid} {
		err := ErrorWithClass("some text", class)
		if err.Error() != "some text" {
			t.Fatalf("text changed: %q", err)
		}
		if got := ReadErrorClass(err); got != class {
			t.Fatalf("class %q round trips as %q", class, got)
		}
	}
	if got := ReadErrorClass(ErrorWithClass("some text", "")); got != ReadClassUnavailable {
		t.Fatalf("empty class must read as unavailable, got %q", got)
	}
}

func TestLegacyScopedAbsentMessageIsNotLatched(t *testing.T) {
	if got := ReadErrorClass(ErrorWithClass("object is absent from snapshot", "")); got != ReadClassAbsent {
		t.Fatalf("legacy absent message = %q, want absent", got)
	}
}
