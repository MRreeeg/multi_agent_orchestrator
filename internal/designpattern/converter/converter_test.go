package converter

import (
	"testing"

	"reasonix/internal/designpattern/matcher"
)

func newTestConverter() *Converter {
	return New(matcher.NewRegistry())
}

func TestToEnglish(t *testing.T) {
	c := newTestConverter()
	got, err := c.ToEnglish("单例模式")
	if err != nil {
		t.Fatalf("ToEnglish(单例模式) = %v", err)
	}
	if got != "Singleton" {
		t.Errorf("got %q, want Singleton", got)
	}
}

func TestToEnglishEnglishInput(t *testing.T) {
	c := newTestConverter()
	got, err := c.ToEnglish("Strategy")
	if err != nil {
		t.Fatalf("ToEnglish(Strategy) = %v", err)
	}
	if got != "Strategy" {
		t.Errorf("got %q, want Strategy", got)
	}
}

func TestToChinese(t *testing.T) {
	c := newTestConverter()
	got, err := c.ToChinese("Singleton")
	if err != nil {
		t.Fatalf("ToChinese(Singleton) = %v", err)
	}
	if got != "单例模式" {
		t.Errorf("got %q, want 单例模式", got)
	}
}

func TestToChineseChineseInput(t *testing.T) {
	c := newTestConverter()
	got, err := c.ToChinese("单例模式")
	if err != nil {
		t.Fatalf("ToChinese(单例模式) = %v", err)
	}
	if got != "单例模式" {
		t.Errorf("got %q, want 单例模式", got)
	}
}

func TestToEnglishNotFound(t *testing.T) {
	c := newTestConverter()
	_, err := c.ToEnglish("xyz_not_a_pattern")
	if err == nil {
		t.Fatal("expected error for unknown pattern")
	}
}

func TestConvertCasePascal(t *testing.T) {
	c := newTestConverter()
	got, err := c.ConvertCase("Factory Method", PascalCase)
	if err != nil {
		t.Fatalf("ConvertCase = %v", err)
	}
	if got != "FactoryMethod" {
		t.Errorf("got %q, want FactoryMethod", got)
	}
}

func TestConvertCaseCamel(t *testing.T) {
	c := newTestConverter()
	got, err := c.ConvertCase("Factory Method", CamelCase)
	if err != nil {
		t.Fatalf("ConvertCase = %v", err)
	}
	if got != "factoryMethod" {
		t.Errorf("got %q, want factoryMethod", got)
	}
}

func TestConvertCaseSnake(t *testing.T) {
	c := newTestConverter()
	got, err := c.ConvertCase("Factory Method", SnakeCase)
	if err != nil {
		t.Fatalf("ConvertCase = %v", err)
	}
	if got != "factory_method" {
		t.Errorf("got %q, want factory_method", got)
	}
}

func TestConvertCaseKebab(t *testing.T) {
	c := newTestConverter()
	got, err := c.ConvertCase("Chain of Responsibility", KebabCase)
	if err != nil {
		t.Fatalf("ConvertCase = %v", err)
	}
	if got != "chain-of-responsibility" {
		t.Errorf("got %q, want chain-of-responsibility", got)
	}
}

func TestConvertCaseUpper(t *testing.T) {
	c := newTestConverter()
	got, err := c.ConvertCase("Factory Method", UpperCase)
	if err != nil {
		t.Fatalf("ConvertCase = %v", err)
	}
	if got != "FACTORY_METHOD" {
		t.Errorf("got %q, want FACTORY_METHOD", got)
	}
}

func TestConvertCaseChineseInput(t *testing.T) {
	c := newTestConverter()
	got, err := c.ConvertCase("单例模式", PascalCase)
	if err != nil {
		t.Fatalf("ConvertCase(单例模式) = %v", err)
	}
	if got != "Singleton" {
		t.Errorf("got %q, want Singleton", got)
	}
}

func TestSplitWords(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"Factory Method", []string{"factory", "method"}},
		{"Chain of Responsibility", []string{"chain", "of", "responsibility"}},
		{"Template Method", []string{"template", "method"}},
		{"Abstract Factory", []string{"abstract", "factory"}},
	}
	for _, tt := range tests {
		got := splitWords(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("splitWords(%q) = %v, want %v (len %d vs %d)", tt.input, got, tt.want, len(got), len(tt.want))
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("splitWords(%q) = %v, want %v", tt.input, got, tt.want)
				break
			}
		}
	}
}

func TestParseCase(t *testing.T) {
	tests := []struct {
		input string
		want  Case
	}{
		{"PascalCase", PascalCase},
		{"pascalcase", PascalCase},
		{"pascal", PascalCase},
		{"camelCase", CamelCase},
		{"camelcase", CamelCase},
		{"camel", CamelCase},
		{"snake_case", SnakeCase},
		{"snake_case ", SnakeCase},
		{"snake", SnakeCase},
		{"kebab-case", KebabCase},
		{"kebab-case ", KebabCase},
		{"kebab", KebabCase},
		{"UPPER_CASE", UpperCase},
		{"original", Original},
	}
	for _, tt := range tests {
		got, err := ParseCase(tt.input)
		if err != nil {
			t.Errorf("ParseCase(%q) error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseCase(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestParseCaseInvalid(t *testing.T) {
	_, err := ParseCase("invalid_case_style")
	if err == nil {
		t.Fatal("expected error for invalid case")
	}
}
