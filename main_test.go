package leaderelection

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// Every leadership term starts two goroutines -- the watchdog (Elector.watch)
// and the one running OnStartedLeading -- and every database call starts a
// third inside the clock's WithTimeout. All of them are supposed to be gone by
// the time Run returns, and nothing in the suite would notice if one were not:
// a leaked watchdog holds a term it can no longer end, and a test that has
// already passed says nothing about it.
//
// So the check is made once, at the end of the binary, against the goroutines
// that existed before any test ran.

func TestMain(m *testing.M) {
	before := runtime.NumGoroutine()
	code := m.Run()
	// Only worth reporting on a run that otherwise passed. After a failure the
	// leak is a symptom, and the failure above it is the diagnosis.
	if code == 0 && !fuzzing() {
		if err := awaitGoroutines(before); err != nil {
			fmt.Fprintln(os.Stderr, err)
			code = 1
		}
	}
	os.Exit(code)
}

// awaitGoroutines waits for the goroutine count to come back down to want,
// giving shutdown paths that are already under way a chance to finish.
func awaitGoroutines(want int) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := runtime.NumGoroutine()
		if got <= want {
			return nil
		}
		if time.Now().After(deadline) {
			var buf stackBuffer
			_ = pprof.Lookup("goroutine").WriteTo(&buf, 1)
			return fmt.Errorf("%d goroutines outlived the tests (%d before, %d after):\n%s",
				got-want, want, got, buf.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// fuzzing reports whether this process is exploring inputs rather than running
// the suite. A fuzzing worker keeps goroutines of its own, and they are not
// this package's to account for.
func fuzzing() bool {
	for _, name := range []string{"test.fuzz", "test.fuzzworker"} {
		if f := flag.Lookup(name); f != nil && f.Value.String() != "" && f.Value.String() != "false" {
			return true
		}
	}
	return false
}

// stackBuffer is the smallest io.Writer that will do. The module has no test
// dependencies, and bytes.Buffer would be one more import for one call.
type stackBuffer []byte

func (b *stackBuffer) Write(p []byte) (int, error) {
	*b = append(*b, p...)
	return len(p), nil
}

func (b *stackBuffer) String() string { return string(*b) }
