// Package converter handles format conversion for design pattern names between
// Chinese and English, and between naming conventions (PascalCase, camelCase,
// snake_case, kebab-case).
package converter

import (
	"fmt"
	"regexp"
	"strings"

	"reasonix/internal/designpattern/errorcode"
	"reasonix/internal/designpattern/matcher"
)

// ---------- Naming convention ----------

// Case represents a naming convention.
type Case int

const (
	Original   Case = iota // leave as-is
	PascalCase             // Singleton, FactoryMethod
	CamelCase              // singleton, factoryMethod
	SnakeCase              // singleton, factory_method
	KebabCase              // singleton, factory-method
	UpperCase              // SINGLETON, FACTORY_METHOD
)

var caseNames = map[Case]string{
	Original:   "original",
	PascalCase: "PascalCase",
	CamelCase:  "camelCase",
	SnakeCase:  "snake_case",
	KebabCase:  "kebab-case",
	UpperCase:  "UPPER_CASE",
}

func (c Case) String() string {
	if name, ok := caseNames[c]; ok {
		return name
	}
	return fmt.Sprintf("Case(%d)", c)
}

// ParseCase parses a case name string (case-insensitive).
// Accepts full names ("snake_case") and short forms ("snake").
func ParseCase(s string) (Case, error) {
	lower := strings.ToLower(strings.TrimSpace(s))
	// Try exact match against canonical names.
	for k, v := range caseNames {
		if strings.ToLower(v) == lower {
			return k, nil
		}
	}
	// Try short-name match.
	short := map[string]Case{
		"pascal": PascalCase,
		"camel":  CamelCase,
		"snake":  SnakeCase,
		"kebab":  KebabCase,
		"upper":  UpperCase,
	}
	if c, ok := short[lower]; ok {
		return c, nil
	}
	return Original, errorcode.New(errorcode.InvalidInput, "unknown naming convention: %q", s)
}

// ---------- Converters ----------

// Converter translates between different design pattern name representations.
type Converter struct {
	registry *matcher.Registry
}

// New builds a Converter that uses the given matcher registry for name lookups.
func New(reg *matcher.Registry) *Converter {
	return &Converter{registry: reg}
}

// ToEnglish converts a pattern reference (Chinese or alias) to its canonical
// English name.
func (c *Converter) ToEnglish(name string) (string, error) {
	result, err := c.registry.Lookup(name)
	if err != nil {
		return "", err
	}
	return result.Pattern.English, nil
}

// ToChinese converts a pattern reference (English or alias) to its canonical
// Chinese name.
func (c *Converter) ToChinese(name string) (string, error) {
	result, err := c.registry.Lookup(name)
	if err != nil {
		return "", err
	}
	return result.Pattern.Chinese, nil
}

// ConvertCase converts a pattern name to the given naming convention.
// A cross-field converting tool for pattern-name-style normalization.
func (c *Converter) ConvertCase(name string, targetCase Case) (string, error) {
	result, err := c.registry.Lookup(name)
	if err != nil {
		return "", err
	}
	return applyCase(result.Pattern.English, targetCase), nil
}

// ---------- Case conversion helpers ----------
//
// Go's RE2 engine does not support lookahead/lookbehind, so we handle camelCase
// boundaries with a separate global replacement instead.

// wordSep matches runs of whitespace, underscores, and hyphens.
var wordSep = regexp.MustCompile(`[\s_-]+`)

// camelEdge matches transitions between lower→upper and upper→upper-lower
// (e.g. "abstractFactory" → "abstract Factory", "XMLParser" → "XML Parser").
// We replace each match with the first rune + space + rest, then split on spaces.
var camelEdge = regexp.MustCompile(`([a-z])([A-Z])|([A-Z])([A-Z][a-z])`)

// splitWords splits a GoF-style pattern name into lowercase words.
// Examples:
//
//	"Factory Method" → ["factory", "method"]
//	"Chain of Responsibility" → ["chain", "of", "responsibility"]
//	"snake_case_name" → ["snake", "case", "name"]
//	"abstractFactory" → ["abstract", "factory"]
func splitWords(name string) []string {
	// Insert a space at camelCase boundaries.
	inserted := camelEdge.ReplaceAllStringFunc(name, func(match string) string {
		return string(match[0]) + " " + match[1:]
	})

	// Split on whitespace/separator runs.
	parts := wordSep.Split(inserted, -1)
	var words []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			words = append(words, strings.ToLower(p))
		}
	}
	return words
}

// applyCase transforms a canonical English name to the target case convention.
func applyCase(name string, target Case) string {
	switch target {
	case Original:
		return name
	case PascalCase:
		return toPascalCase(name)
	case CamelCase:
		return toCamelCase(name)
	case SnakeCase:
		words := splitWords(name)
		return strings.Join(words, "_")
	case KebabCase:
		words := splitWords(name)
		return strings.Join(words, "-")
	case UpperCase:
		words := splitWords(name)
		return strings.ToUpper(strings.Join(words, "_"))
	default:
		return name
	}
}

func toPascalCase(name string) string {
	words := splitWords(name)
	for i, w := range words {
		words[i] = strings.ToUpper(w[:1]) + w[1:]
	}
	return strings.Join(words, "")
}

func toCamelCase(name string) string {
	words := splitWords(name)
	for i, w := range words {
		if i == 0 {
			words[i] = strings.ToLower(w)
		} else {
			words[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	return strings.Join(words, "")
}
