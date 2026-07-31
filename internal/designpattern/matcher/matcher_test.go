package matcher

import (
	"testing"

	"reasonix/internal/designpattern/errorcode"
)

func TestLookupExactEnglish(t *testing.T) {
	r := NewRegistry()
	result, err := r.Lookup("Singleton")
	if err != nil {
		t.Fatalf("Lookup(Singleton) = %v", err)
	}
	if result.Pattern.English != "Singleton" {
		t.Errorf("got English=%q, want Singleton", result.Pattern.English)
	}
	if result.Score != 100 {
		t.Errorf("got Score=%d, want 100", result.Score)
	}
}

func TestLookupExactChinese(t *testing.T) {
	r := NewRegistry()
	result, err := r.Lookup("单例模式")
	if err != nil {
		t.Fatalf("Lookup(单例模式) = %v", err)
	}
	if result.Pattern.Chinese != "单例模式" {
		t.Errorf("got Chinese=%q, want 单例模式", result.Pattern.Chinese)
	}
	if result.Score != 100 {
		t.Errorf("got Score=%d, want 100", result.Score)
	}
}

func TestLookupAlias(t *testing.T) {
	r := NewRegistry()
	result, err := r.Lookup("工厂方法")
	if err != nil {
		t.Fatalf("Lookup(工厂方法) = %v", err)
	}
	if result.Pattern.English != "Factory Method" {
		t.Errorf("got English=%q, want Factory Method", result.Pattern.English)
	}
}

func TestLookupCaseInsensitive(t *testing.T) {
	r := NewRegistry()
	result, err := r.Lookup("singleton")
	if err != nil {
		t.Fatalf("Lookup(singleton) = %v", err)
	}
	if result.Pattern.English != "Singleton" {
		t.Errorf("got English=%q, want Singleton", result.Pattern.English)
	}
}

func TestLookupNotFound(t *testing.T) {
	r := NewRegistry()
	_, err := r.Lookup("NonExistentPatternXYZ")
	if err == nil {
		t.Fatal("expected error for non-existent pattern")
	}
	if k, _ := errorcode.KindOf(err); k != errorcode.NotFound {
		t.Errorf("expected Kind NotFound, got %d", k)
	}
}

func TestLookupAliasFactory(t *testing.T) {
	r := NewRegistry()
	// "Factory" is an alias of "Factory Method" only.
	result, err := r.Lookup("Factory")
	if err != nil {
		t.Fatalf("Lookup(Factory) = %v", err)
	}
	if result.Pattern.English != "Factory Method" {
		t.Errorf("got English=%q, want Factory Method", result.Pattern.English)
	}
}

func TestFuzzyPrefixMatch(t *testing.T) {
	r := NewRegistry()
	result, err := r.Lookup("Single")
	if err != nil {
		t.Fatalf("Lookup(Single) = %v", err)
	}
	if result.Pattern.English != "Singleton" {
		t.Errorf("got English=%q, want Singleton", result.Pattern.English)
	}
}

func TestList(t *testing.T) {
	r := NewRegistry()
	all := r.List()
	if len(all) == 0 {
		t.Fatal("List() returned empty")
	}
	// Verify sorted.
	for i := 1; i < len(all); i++ {
		if all[i-1].English > all[i].English {
			t.Errorf("List not sorted: %s > %s", all[i-1].English, all[i].English)
		}
	}
}

func TestListByCategory(t *testing.T) {
	r := NewRegistry()
	byCat := r.ListByCategory()
	if len(byCat) != 3 {
		t.Errorf("expected 3 categories, got %d", len(byCat))
	}
	for _, cat := range []string{"creational", "structural", "behavioural"} {
		if _, ok := byCat[cat]; !ok {
			t.Errorf("missing category %q", cat)
		}
	}
}

func TestLookupWithSpaces(t *testing.T) {
	r := NewRegistry()
	result, err := r.Lookup("FactoryMethod")
	if err != nil {
		t.Fatalf("Lookup(FactoryMethod) = %v", err)
	}
	if result.Pattern.English != "Factory Method" {
		t.Errorf("got English=%q, want Factory Method", result.Pattern.English)
	}
}
