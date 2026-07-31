package errorcode

import (
	"errors"
	"testing"
)

func TestKindOf(t *testing.T) {
	t.Run("nil returns Internal", func(t *testing.T) {
		k, err := KindOf(nil)
		if k != Internal {
			t.Errorf("KindOf(nil) = %d, want %d", k, Internal)
		}
		if err != nil {
			t.Errorf("KindOf(nil) err = %v, want nil", err)
		}
	})

	t.Run("typed error", func(t *testing.T) {
		orig := New(NotFound, "pattern not found: %s", "Singleton")
		k, err := KindOf(orig)
		if k != NotFound {
			t.Errorf("KindOf = %d, want %d", k, NotFound)
		}
		if !errors.Is(err, orig) {
			t.Error("returned error should be the original")
		}
	})

	t.Run("wrapped typed error", func(t *testing.T) {
		inner := New(Ambiguous, "matches multiple patterns")
		wrapped := &wrapError{inner: inner}
		k, e := KindOf(wrapped)
		if k != Ambiguous {
			t.Errorf("KindOf = %d, want %d", k, Ambiguous)
		}
		if !errors.Is(e, inner) {
			t.Error("should unwrap to inner")
		}
	})

	t.Run("plain error returns Internal", func(t *testing.T) {
		k, err := KindOf(errors.New("plain"))
		if k != Internal {
			t.Errorf("KindOf = %d, want %d", k, Internal)
		}
		if err == nil {
			t.Error("should return the original error")
		}
	})
}

func TestKindString(t *testing.T) {
	tests := []struct {
		kind Kind
		want string
	}{
		{Internal, "internal"},
		{NotFound, "not_found"},
		{Ambiguous, "ambiguous"},
		{InvalidInput, "invalid_input"},
		{Semantic, "semantic_error"},
		{Conversion, "conversion_error"},
		{Unsupported, "unsupported"},
		{Kind(99), "unknown(99)"},
	}
	for _, tt := range tests {
		if got := tt.kind.String(); got != tt.want {
			t.Errorf("Kind(%d).String() = %q, want %q", tt.kind, got, tt.want)
		}
	}
}

// wrapError is a test helper that wraps an *Error to test the unwrap logic.
type wrapError struct {
	inner error
}

func (w *wrapError) Error() string { return "wrap: " + w.inner.Error() }
func (w *wrapError) Unwrap() error { return w.inner }
