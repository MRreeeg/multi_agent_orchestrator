// Package errorcode defines typed, structured error codes for the design-pattern
// translation and validation system. Every error that crosses a package boundary
// uses one of these codes so callers can switch on Kind without parsing strings.
package errorcode

import "fmt"

// Kind classifies a design-pattern error.
type Kind int

const (
	// Internal signals an unexpected internal failure (should never happen).
	Internal Kind = iota
	// NotFound means a requested pattern name or definition does not exist.
	NotFound
	// Ambiguous means the input matched more than one pattern.
	Ambiguous
	// InvalidInput means the provided value is syntactically invalid.
	InvalidInput
	// Semantic means the LLM-based semantic check failed or returned an
	// unexpected result.
	Semantic
	// Conversion means a format conversion (e.g. Chinese→English) could not
	// be completed.
	Conversion
	// Unsupported means the requested operation is not implemented.
	Unsupported
)

// Error carries a machine-readable Kind and a human-readable message.
type Error struct {
	Kind    Kind
	Message string
}

func (e *Error) Error() string { return e.Message }

// Unwrap is a no-op that satisfies the unwrap interface so errors.Is / errors.As
// work naturally.
func (e *Error) Unwrap() error { return nil }

// New builds a new *Error with the given kind and formatted message.
func New(kind Kind, format string, args ...any) error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

// KindOf extracts the Kind from err. If err is nil it returns (Internal, nil).
// If err is not an *Error it returns (Internal, err) so callers always get a
// Kind back.
func KindOf(err error) (Kind, error) {
	if err == nil {
		return Internal, nil
	}
	// Walk the error chain looking for an *Error.
	current := err
	for {
		if as, ok := current.(*Error); ok { //nolint:errorlint // we own the type
			return as.Kind, as
		}
		u, ok := current.(interface{ Unwrap() error })
		if !ok {
			break
		}
		current = u.Unwrap()
	}
	return Internal, err
}

// String returns the English label for a Kind.
func (k Kind) String() string {
	switch k {
	case Internal:
		return "internal"
	case NotFound:
		return "not_found"
	case Ambiguous:
		return "ambiguous"
	case InvalidInput:
		return "invalid_input"
	case Semantic:
		return "semantic_error"
	case Conversion:
		return "conversion_error"
	case Unsupported:
		return "unsupported"
	default:
		return fmt.Sprintf("unknown(%d)", k)
	}
}
