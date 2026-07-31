package cli

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"reasonix/internal/memory"
)

// memoryForm is the interactive wizard for "/remember" with no arguments.
// It guides the user through filling in the fields of a memory.Memory one at a
// time, using the existing textarea for input. On the final step Enter submits
// the memory; Esc cancels at any step.
type memoryForm struct {
	values []string     // one per step; empty until the user fills it
	step   int          // current step index (0..len(steps)-1)
	steps  []memoryStep // the field descriptors
	done   bool         // true after successful save
	path   string       // file path returned by SaveMemory
	err    string       // validation / save error, "" when clean
}

type memoryStep struct {
	Label string // prompt shown above the input, e.g. "Name (kebab-case)"
	Key   string // field key for summary display
}

func newMemoryForm() *memoryForm {
	return &memoryForm{
		values: make([]string, 5),
		steps: []memoryStep{
			{Label: "Name (kebab-case, e.g. uses-tabs)", Key: "name"},
			{Label: "Title (human-readable label, e.g. Prefers tabs)", Key: "title"},
			{Label: "Description (one-line summary for the index)", Key: "description"},
			{Label: "Type [project / user / feedback / reference]", Key: "type"},
			{Label: "Body (Markdown — what you want to remember)", Key: "body"},
		},
	}
}

// currentLabel returns the prompt label for the current step.
func (f *memoryForm) currentLabel() string {
	if f.step >= 0 && f.step < len(f.steps) {
		return f.steps[f.step].Label
	}
	return ""
}

// allFilled returns true when every step has a non-empty value.
func (f *memoryForm) allFilled() bool {
	for i, v := range f.values {
		if strings.TrimSpace(v) == "" && i < len(f.steps) {
			return false
		}
	}
	return true
}

// toMemory builds a memory.Memory from the filled values.
func (f *memoryForm) toMemory() memory.Memory {
	m := memory.Memory{
		Name:        strings.TrimSpace(f.values[0]),
		Title:       strings.TrimSpace(f.values[1]),
		Description: strings.TrimSpace(f.values[2]),
		Body:        strings.TrimSpace(f.values[4]),
	}
	m.Type = memory.NormalizeType(f.values[3])
	return m
}

// renderFormCard renders the form overlay card for display in the TUI.
func (f *memoryForm) renderFormCard(width int) string {
	if f == nil {
		return ""
	}
	w := max(viewWidth(width), 40)
	var b strings.Builder

	b.WriteString(lipgloss.NewStyle().Bold(true).Render("📝 New Memory") + "\n")

	// Show each completed field as a summary row.
	for i, s := range f.steps {
		if i >= len(f.values) {
			break
		}
		val := f.values[i]
		active := i == f.step
		var line string
		if val != "" {
			// Truncate long values for display.
			display := val
			if len(display) > 50 {
				display = display[:47] + "..."
			}
			if strings.Contains(s.Key, "body") && len(val) > 50 {
				display = fmt.Sprintf("%d chars", len(val))
			}
			line = fmt.Sprintf("✓ %s: %s", s.Key, display)
		} else if active {
			line = fmt.Sprintf("▶ %s", s.Label)
		} else {
			line = fmt.Sprintf("○ %s", s.Key)
		}

		if active {
			line = lipgloss.NewStyle().Foreground(lipgloss.Color("212")).Render(line)
		} else if val != "" {
			line = lipgloss.NewStyle().Foreground(lipgloss.Color("42")).Render(line)
		} else {
			line = lipgloss.NewStyle().Foreground(lipgloss.Color("243")).Render(line)
		}
		b.WriteString(line + "\n")
	}

	b.WriteString("\n")
	if f.err != "" {
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Render("⚠ "+f.err) + "\n\n")
	}

	if f.allFilled() {
		b.WriteString(viewMeta("All fields filled! Press Enter to save, or Esc to cancel."))
	} else {
		b.WriteString(viewMeta("Type your answer and press Enter. Tab to next field, Esc to cancel."))
	}

	return choicePanelStyle.Width(w).Render(b.String())
}

// handleMemoryFormEnter is called when Enter is pressed while m.memoryForm is
// active. It captures the textarea value into the current step, then either
// advances to the next step or saves the memory when all fields are filled.
// The textarea is cleared after each step.
func (m *chatTUI) handleMemoryFormEnter() (tea.Model, tea.Cmd) {
	f := m.memoryForm
	if f == nil {
		return m, nil
	}

	val := strings.TrimSpace(m.input.Value())
	if val == "" && f.step < len(f.steps) {
		// Skip empty fields.
	} else {
		f.values[f.step] = val
	}

	m.input.Reset()
	m.pastedBlocks = nil

	if f.allFilled() {
		return m.saveAndCloseMemoryForm()
	}

	// Advance to the next unfilled step.
	for f.step < len(f.steps)-1 {
		f.step++
		if strings.TrimSpace(f.values[f.step]) == "" {
			break
		}
	}

	if f.allFilled() {
		return m.saveAndCloseMemoryForm()
	}

	return m, nil
}

// saveAndCloseMemoryForm saves the memory and closes the form overlay.
func (m *chatTUI) saveAndCloseMemoryForm() (tea.Model, tea.Cmd) {
	f := m.memoryForm
	if f == nil {
		return m, nil
	}

	mem := f.toMemory()
	if mem.Name == "" {
		f.err = "Name is required"
		return m, nil
	}

	path, err := m.ctrl.SaveMemory(mem)
	if err != nil {
		f.err = err.Error()
		return m, nil
	}

	m.memoryForm = nil
	m.notice("memory saved → " + path)
	return m, nil
}

// renderMemoryFormCard renders the memory form card for the TUI bottom panel.
func (m chatTUI) renderMemoryFormCard() string {
	if m.memoryForm == nil {
		return ""
	}
	return m.memoryForm.renderFormCard(m.width)
}
