// Package assert holds the handful of test helpers the core module uses.
//
// It exists so that the root module keeps a completely empty dependency list,
// test dependencies included. Anything more elaborate than this belongs in a
// test, not in a helper.
package assert

import (
	"errors"
	"reflect"
	"testing"
)

// Equal fails the test unless got and want are equal.
func Equal[T comparable](t testing.TB, got, want T, what string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

// DeepEqual fails the test unless got and want are deeply equal.
func DeepEqual(t testing.TB, got, want any, what string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: got %#v, want %#v", what, got, want)
	}
}

// True fails the test unless cond holds.
func True(t testing.TB, cond bool, what string) {
	t.Helper()
	if !cond {
		t.Fatalf("%s: got false, want true", what)
	}
}

// False fails the test unless cond does not hold.
func False(t testing.TB, cond bool, what string) {
	t.Helper()
	if cond {
		t.Fatalf("%s: got true, want false", what)
	}
}

// NoError fails the test if err is non-nil.
func NoError(t testing.TB, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", what, err)
	}
}

// ErrorIs fails the test unless err matches target under [errors.Is].
func ErrorIs(t testing.TB, err, target error, what string) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("%s: got error %v, want %v", what, err, target)
	}
}

// Error fails the test if err is nil.
func Error(t testing.TB, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: got nil error, want non-nil", what)
	}
}
