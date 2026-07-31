// Package matcher provides a registry of known GoF-style design pattern names in
// both English and Chinese, and a fuzzy matcher that resolves user-provided
// pattern references to a canonical definition.
package matcher

import (
	"sort"
	"strings"

	"reasonix/internal/designpattern/errorcode"
)

// Pattern holds the canonical name and known aliases for a design pattern.
type Pattern struct {
	// English is the canonical English name (e.g. "Singleton").
	English string
	// Chinese is the canonical Chinese translation (e.g. "单例模式").
	Chinese string
	// Aliases are additional known names (abbreviations, alternative names).
	Aliases []string
	// Category classifies the pattern (creational / structural / behavioural).
	Category string
}

// MatchResult is returned by a successful match.
type MatchResult struct {
	Pattern Pattern
	Score   int // higher = better match; 100 = exact
	Matched string
}

// Registry holds all known patterns and exposes lookup helpers.
type Registry struct {
	patterns []Pattern
	byLower  map[string][]Pattern // lowercased name → candidates
}

// NewRegistry builds a Registry populated with the standard GoF patterns.
func NewRegistry() *Registry {
	reg := &Registry{
		byLower: make(map[string][]Pattern),
	}
	reg.patterns = defaultPatterns()
	for _, p := range reg.patterns {
		reg.index(p)
	}
	return reg
}

// index inserts one pattern into the byLower map under every known name.
func (r *Registry) index(p Pattern) {
	add := func(name string) {
		key := strings.ToLower(strings.ReplaceAll(name, " ", ""))
		r.byLower[key] = append(r.byLower[key], p)
	}
	add(p.English)
	add(p.Chinese)
	for _, a := range p.Aliases {
		add(a)
	}
}

// Lookup attempts to find a pattern by exact (case-insensitive, space-insensitive)
// match on English, Chinese, or any alias. Returns an *errorcode.Error with Kind
// NotFound when no match exists, or Ambiguous when multiple patterns match.
func (r *Registry) Lookup(name string) (MatchResult, error) {
	key := strings.ToLower(strings.ReplaceAll(name, " ", ""))
	candidates := r.byLower[key]
	switch len(candidates) {
	case 0:
		// Fall back to substring matching.
		return r.fuzzyLookup(name)
	case 1:
		return MatchResult{Pattern: candidates[0], Score: 100, Matched: name}, nil
	default:
		return MatchResult{}, errorcode.New(errorcode.Ambiguous,
			"name %q matches %d patterns: %v", name, len(candidates), patternNames(candidates))
	}
}

// fuzzyLookup tries substring matching on all known names. Returns the best match
// or NotFound.
func (r *Registry) fuzzyLookup(name string) (MatchResult, error) {
	lower := strings.ToLower(name)
	var best *MatchResult
	for _, p := range r.patterns {
		names := append([]string{p.English, p.Chinese}, p.Aliases...)
		for _, n := range names {
			nl := strings.ToLower(n)
			score := similarityScore(lower, nl)
			if score > 0 && (best == nil || score > best.Score) {
				best = &MatchResult{Pattern: p, Score: score, Matched: n}
			}
		}
	}
	if best == nil {
		return MatchResult{}, errorcode.New(errorcode.NotFound,
			"no pattern matches %q", name)
	}
	return *best, nil
}

// similarityScore returns a simple substring-based score: 100 for exact, 80 for
// prefix-match, 60 for contains, 40 for word-boundary match.
func similarityScore(input, candidate string) int {
	if input == candidate {
		return 100
	}
	if strings.HasPrefix(candidate, input) {
		return 80
	}
	if strings.Contains(candidate, input) {
		return 60
	}
	// Word-boundary: each word in the input matches somewhere in the candidate.
	words := strings.FieldsFunc(input, func(r rune) bool {
		return r == ' ' || r == '-' || r == '_'
	})
	if len(words) > 1 {
		matchedAll := true
		for _, w := range words {
			if !strings.Contains(candidate, w) {
				matchedAll = false
				break
			}
		}
		if matchedAll {
			return 40
		}
	}
	return 0
}

// List returns a sorted copy of all registered patterns.
func (r *Registry) List() []Pattern {
	out := append([]Pattern(nil), r.patterns...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].English < out[j].English
	})
	return out
}

// ListByCategory returns patterns grouped by category.
func (r *Registry) ListByCategory() map[string][]Pattern {
	m := make(map[string][]Pattern)
	for _, p := range r.patterns {
		m[p.Category] = append(m[p.Category], p)
	}
	for _, v := range m {
		sort.Slice(v, func(i, j int) bool {
			return v[i].English < v[j].English
		})
	}
	return m
}

func patternNames(ps []Pattern) []string {
	names := make([]string, len(ps))
	for i, p := range ps {
		names[i] = p.English + "/" + p.Chinese
	}
	return names
}

// defaultPatterns returns the standard 23 GoF patterns with English and Chinese
// names.
func defaultPatterns() []Pattern {
	return []Pattern{
		// Creational
		{English: "Singleton", Chinese: "单例模式", Aliases: []string{"单例"}, Category: "creational"},
		{English: "Factory Method", Chinese: "工厂方法模式", Aliases: []string{"工厂方法", "Factory"}, Category: "creational"},
		{English: "Abstract Factory", Chinese: "抽象工厂模式", Aliases: []string{"抽象工厂"}, Category: "creational"},
		{English: "Builder", Chinese: "建造者模式", Aliases: []string{"生成器模式", "建造者"}, Category: "creational"},
		{English: "Prototype", Chinese: "原型模式", Aliases: []string{"原型"}, Category: "creational"},

		// Structural
		{English: "Adapter", Chinese: "适配器模式", Aliases: []string{"适配器", "Wrapper", "包装器"}, Category: "structural"},
		{English: "Bridge", Chinese: "桥接模式", Aliases: []string{"桥接"}, Category: "structural"},
		{English: "Composite", Chinese: "组合模式", Aliases: []string{"组合"}, Category: "structural"},
		{English: "Decorator", Chinese: "装饰器模式", Aliases: []string{"装饰器", "装饰"}, Category: "structural"},
		{English: "Facade", Chinese: "外观模式", Aliases: []string{"外观", "门面模式"}, Category: "structural"},
		{English: "Flyweight", Chinese: "享元模式", Aliases: []string{"享元", "轻量级模式"}, Category: "structural"},
		{English: "Proxy", Chinese: "代理模式", Aliases: []string{"代理"}, Category: "structural"},

		// Behavioural
		{English: "Chain of Responsibility", Chinese: "责任链模式", Aliases: []string{"责任链", "Chain of Resp"}, Category: "behavioural"},
		{English: "Command", Chinese: "命令模式", Aliases: []string{"命令"}, Category: "behavioural"},
		{English: "Interpreter", Chinese: "解释器模式", Aliases: []string{"解释器"}, Category: "behavioural"},
		{English: "Iterator", Chinese: "迭代器模式", Aliases: []string{"迭代器"}, Category: "behavioural"},
		{English: "Mediator", Chinese: "中介者模式", Aliases: []string{"中介者"}, Category: "behavioural"},
		{English: "Memento", Chinese: "备忘录模式", Aliases: []string{"备忘录"}, Category: "behavioural"},
		{English: "Observer", Chinese: "观察者模式", Aliases: []string{"观察者", "发布订阅"}, Category: "behavioural"},
		{English: "State", Chinese: "状态模式", Aliases: []string{"状态"}, Category: "behavioural"},
		{English: "Strategy", Chinese: "策略模式", Aliases: []string{"策略"}, Category: "behavioural"},
		{English: "Template Method", Chinese: "模板方法模式", Aliases: []string{"模板方法", "Template"}, Category: "behavioural"},
		{English: "Visitor", Chinese: "访问者模式", Aliases: []string{"访问者"}, Category: "behavioural"},
	}
}
