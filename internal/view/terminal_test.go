package view

import (
	"strings"
	"testing"
	"time"

	"reasonix/internal/jobs"
)

func TestShow_EmptyJobs(t *testing.T) {
	var buf strings.Builder
	v := View{}
	v.Show(&buf, nil)
	if got := buf.String(); got != "" {
		t.Errorf("Show with nil jobs = %q, want empty", got)
	}

	buf.Reset()
	v.Show(&buf, []jobs.View{})
	if got := buf.String(); got != "" {
		t.Errorf("Show with empty jobs = %q, want empty", got)
	}
}

func TestShow_SingleJob(t *testing.T) {
	now := time.Now()
	jv := []jobs.View{
		{ID: "bash-1", Kind: "bash", Label: "build", Status: "running", StartedAt: now.Add(-5 * time.Second).UnixMilli()},
	}

	var buf strings.Builder
	v := View{ColorEnabled: false}
	v.Show(&buf, jv)

	got := buf.String()
	if !strings.Contains(got, "bash") {
		t.Errorf("Show output missing kind %q; got:\n%s", "bash", got)
	}
	if !strings.Contains(got, "bash-1") {
		t.Errorf("Show output missing id %q; got:\n%s", "bash-1", got)
	}
	if !strings.Contains(got, "build") {
		t.Errorf("Show output missing label %q; got:\n%s", "build", got)
	}
	if !strings.Contains(got, "5s") {
		t.Errorf("Show output missing elapsed time; got:\n%s", got)
	}
}

func TestShow_MultipleJobsGroupedByKind(t *testing.T) {
	now := time.Now()
	jv := []jobs.View{
		{ID: "bash-1", Kind: "bash", Label: "lint", Status: "running", StartedAt: now.Add(-3 * time.Second).UnixMilli()},
		{ID: "task-1", Kind: "task", Label: "analysis", Status: "running", StartedAt: now.Add(-10 * time.Second).UnixMilli()},
		{ID: "bash-2", Kind: "bash", Label: "test", Status: "running", StartedAt: now.Add(-1 * time.Second).UnixMilli()},
	}

	var buf strings.Builder
	v := View{ColorEnabled: false}
	v.Show(&buf, jv)

	got := buf.String()
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 lines, got %d:\n%s", len(lines), got)
	}

	// All job IDs should appear
	for _, j := range jv {
		if !strings.Contains(got, j.ID) {
			t.Errorf("Show output missing job %q; got:\n%s", j.ID, got)
		}
	}
}

func TestShow_JobWithoutLabel(t *testing.T) {
	now := time.Now()
	jv := []jobs.View{
		{ID: "bash-3", Kind: "bash", Label: "", Status: "running", StartedAt: now.Add(-2 * time.Second).UnixMilli()},
	}

	var buf strings.Builder
	v := View{ColorEnabled: false}
	v.Show(&buf, jv)

	got := buf.String()
	if !strings.Contains(got, "bash-3") {
		t.Errorf("Show output missing id; got:\n%s", got)
	}
}

func TestShow_ColorEnabled(t *testing.T) {
	now := time.Now()
	jv := []jobs.View{
		{ID: "bash-1", Kind: "bash", Status: "running", StartedAt: now.Add(-5 * time.Second).UnixMilli()},
	}

	var bufColored strings.Builder
	v := View{ColorEnabled: true}
	v.Show(&bufColored, jv)

	var bufPlain strings.Builder
	v2 := View{ColorEnabled: false}
	v2.Show(&bufPlain, jv)

	colored := bufColored.String()
	plain := bufPlain.String()

	// Colored output should contain ANSI escape sequences (\033[)
	if !strings.Contains(colored, "\033[") {
		t.Error("ColorEnabled=true output should contain ANSI escape sequences")
	}

	// Plain output should NOT contain escape sequences
	if strings.Contains(plain, "\033[") {
		t.Error("ColorEnabled=false output should NOT contain ANSI escape sequences")
	}
}

func TestShow_ColorIndicator(t *testing.T) {
	now := time.Now()
	jv := []jobs.View{
		{ID: "task-2", Kind: "task", Status: "running", StartedAt: now.Add(-30 * time.Second).UnixMilli()},
	}

	var buf strings.Builder
	v := View{ColorEnabled: false}
	v.Show(&buf, jv)

	got := buf.String()
	// Plain mode should use ">" as indicator
	if !strings.Contains(got, ">") {
		t.Errorf("plain mode should show '>' indicator; got:\n%s", got)
	}
}
