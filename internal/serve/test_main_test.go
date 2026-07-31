package serve

import (
	"os"
	"testing"
)

// TestMain keeps serve tests isolated from the user's persistent orchestrator
// history. Handler construction intentionally reloads the store, so sharing
// the real data directory makes tests scan thousands of production entities.
func TestMain(m *testing.M) {
	root, err := os.MkdirTemp("", "reasonix-serve-tests-")
	if err != nil {
		panic(err)
	}
	if err := os.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", root); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(root)
	os.Exit(code)
}
