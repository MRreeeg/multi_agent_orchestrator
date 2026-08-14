package orchestrator

import (
	"strings"
	"testing"
)

func TestDshValidationRunOnly(t *testing.T) {
	ws := t.TempDir()
	if err := validateNodeExecutionConfigAtWorkspaceWithRoute(ExecutorDsh, "run", "deepseek-v4-flash", "", "", ws); err != nil {
		t.Fatalf("dsh run should be valid: %v", err)
	}
	// Empty mode defaults to run for dsh (headless is one-shot).
	if err := validateNodeExecutionConfigAtWorkspaceWithRoute(ExecutorDsh, "", "deepseek-v4-flash", "", "", ws); err != nil {
		t.Fatalf("dsh empty mode should default to run: %v", err)
	}
	// Empty model is valid: DSH owns its own default model.
	if err := validateNodeExecutionConfigAtWorkspaceWithRoute(ExecutorDsh, "run", "", "", "", ws); err != nil {
		t.Fatalf("dsh empty model should be valid: %v", err)
	}
	// serve must be rejected.
	if err := validateNodeExecutionConfigAtWorkspaceWithRoute(ExecutorDsh, "serve", "deepseek-v4-flash", "", "", ws); err == nil {
		t.Fatal("dsh serve should be rejected")
	} else if !strings.Contains(err.Error(), "run") {
		t.Errorf("serve error = %v, want run hint", err)
	}
	// providerRoute must be rejected (DSH routes providers itself).
	if err := validateNodeExecutionConfigAtWorkspaceWithRoute(ExecutorDsh, "run", "deepseek-v4-flash", "", "ccswitch", ws); err == nil {
		t.Fatal("dsh providerRoute should be rejected")
	}
}

func TestDshModelRefPassThrough(t *testing.T) {
	got := resolveExecutorModelRef(t.TempDir(), ExecutorDsh, "run", "deepseek-v4-flash")
	if got != "deepseek-v4-flash" {
		t.Errorf("resolveExecutorModelRef = %q, want deepseek-v4-flash", got)
	}
}

func TestDshRegisteredInExecutors(t *testing.T) {
	if getExecutor(ExecutorDsh) == nil {
		t.Fatal("ExecutorDsh not registered in executors map")
	}
}

func TestNodeTypeCatalogExposesDsh(t *testing.T) {
	catalog := NodeTypeCatalog()
	if len(catalog) == 0 {
		t.Fatal("empty NodeTypeCatalog")
	}
	for _, info := range catalog {
		found := false
		for _, ex := range info.Executors {
			if ex == ExecutorDsh {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: dsh missing from Executors", info.Type)
		}
		models, ok := info.ModelsByExecutor[ExecutorDsh]
		if !ok || len(models) == 0 {
			t.Errorf("%s: dsh missing from ModelsByExecutor", info.Type)
		}
	}
}
