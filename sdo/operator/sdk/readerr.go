package sdk

import "errors"

// A read answers one of four things, and a live view, the memory host and a
// replay give the same answer for the same world:
//
//   - observed: SDO saw the state, including "it is not there" (an absent object
//     is a value, see ErrObjectAbsent); a program may reason on it.
//   - refused: the target answered and said no (a database login was rejected);
//     a program may reason on it and try something else.
//   - unavailable: SDO could not observe (no client, kind not captured, timeout,
//     no accepted source); a program may only treat it as "I do not know".
//   - invalid: the program asked something illegal (a Secret read, a ref outside
//     the namespace, the read budget); a program bug.
//
// An error that carries no class is unavailable: fail closed.
var (
	// ErrUnavailable marks a read that could not observe the state.
	ErrUnavailable = errors.New("read unavailable")
	// ErrRefused marks a read the target itself answered with a refusal.
	ErrRefused = errors.New("read refused by target")
	// ErrInvalidRead marks a read the program was not allowed to make.
	ErrInvalidRead = errors.New("invalid read")
)

// Read error classes, as ReadErrorClass names them and recordings store them.
const (
	ReadClassAbsent      = "absent"
	ReadClassRefused     = "refused"
	ReadClassUnavailable = "unavailable"
	ReadClassInvalid     = "invalid"
)

// ReadErrorClass names an error's class: absent, refused, unavailable or
// invalid. A nil error has no class. When an error carries several, the least
// permissive one wins (invalid, then unavailable, then refused, then absent),
// so a program is never given more freedom than its worst read allowed.
func ReadErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrInvalidRead):
		return ReadClassInvalid
	case errors.Is(err, ErrUnavailable):
		return ReadClassUnavailable
	case errors.Is(err, ErrRefused):
		return ReadClassRefused
	case errors.Is(err, ErrObjectAbsent):
		return ReadClassAbsent
	}
	return ReadClassUnavailable
}

// ErrorWithClass rebuilds a recorded read error: the same text, wrapping the
// sentinel of class, so a replayed failure classifies exactly as it did live.
// An unknown or empty class is a legacy recording and reads as unavailable,
// except the SDK's own absent message, which was always an observation.
func ErrorWithClass(text, class string) error {
	switch class {
	case ReadClassRefused:
		return &classed{text, ErrRefused}
	case ReadClassInvalid:
		return &classed{text, ErrInvalidRead}
	case ReadClassAbsent:
		return &classed{text, ErrObjectAbsent}
	case ReadClassUnavailable:
		return &classed{text, ErrUnavailable}
	}
	if text == ErrObjectAbsent.Error() {
		return &classed{text, ErrObjectAbsent}
	}
	return &classed{text, ErrUnavailable}
}

// Classified returns a sentinel error with its own text that also matches class
// (ErrUnavailable, ErrRefused or ErrInvalidRead) under errors.Is, without
// changing the text readers and recordings already show.
func Classified(text string, class error) error { return &classed{text, class} }

type classed struct {
	text  string
	class error
}

func (e *classed) Error() string { return e.text }
func (e *classed) Unwrap() error { return e.class }
