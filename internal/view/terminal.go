// Package view provides terminal-friendly rendering of Reasonix runtime state.
// It translates internal data structures (background job views, pipeline run
// states, etc.) into formatted terminal output suitable for display in the CLI
// TUI status bar, the /jobs command, or the orchestration console.
package view

import (
	"fmt"
	"io"
	"strings"
	"time"

	"reasonix/internal/jobs"
)

// View renders a collection of background job snapshots as terminal output.
// It provides a Show method that writes a formatted, color-safe representation
// of job status to a given writer.
type View struct {
	// ColorEnabled controls ANSI color output. When false (e.g. piped or
	// NO_COLOR set), Show emits plain text without escape sequences.
	ColorEnabled bool
}

// Show writes a formatted terminal display of the given jobs to w.
//
// Each running job is rendered on its own line as:
//
//	◉ <kind> <id>  <label>  <elapsed>
//
// When there are no jobs, nothing is written. When color is enabled, the
// indicator and elapsed time use dim styling; the job kind uses cyan styling.
func (v View) Show(w io.Writer, jv []jobs.View) {
	if len(jv) == 0 {
		return
	}

	now := time.Now()
	var b strings.Builder

	// group by kind for a compact display
	type group struct {
		Kind string
		Jobs []jobs.View
	}
	groups := make([]group, 0, 4)
	kindIdx := make(map[string]int)
	for _, j := range jv {
		if idx, ok := kindIdx[j.Kind]; ok {
			groups[idx].Jobs = append(groups[idx].Jobs, j)
		} else {
			kindIdx[j.Kind] = len(groups)
			groups = append(groups, group{Kind: j.Kind, Jobs: []jobs.View{j}})
		}
	}

	for _, g := range groups {
		for _, j := range g.Jobs {
			started := time.UnixMilli(j.StartedAt)
			elapsed := now.Sub(started).Truncate(time.Second)

			b.WriteString(v.indicator())
			b.WriteByte(' ')
			b.WriteString(v.kindText(g.Kind))
			b.WriteByte(' ')
			b.WriteString(j.ID)

			if j.Label != "" {
				b.WriteString("  ")
				b.WriteString(j.Label)
			}

			b.WriteString("  ")
			b.WriteString(v.dimText(elapsed.String()))

			b.WriteByte('\n')
		}
	}

	fmt.Fprint(w, b.String())
}

func (v View) indicator() string {
	if !v.ColorEnabled {
		return ">"
	}
	return "\033[2m◉\033[0m"
}

func (v View) dimText(s string) string {
	if !v.ColorEnabled {
		return s
	}
	return "\033[2m" + s + "\033[0m"
}

func (v View) kindText(s string) string {
	if !v.ColorEnabled {
		return s
	}
	return "\033[38;5;44m" + s + "\033[0m"
}
