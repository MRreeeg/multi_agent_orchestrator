// Command run_dp demonstrates and exercises the internal/designpattern packages
// from the command line. It supports lookup, listing, translation, and case
// conversion of GoF design pattern names.
//
// Usage:
//
//	run_dp lookup <name>            — resolve a pattern name (English/Chinese/alias)
//	run_dp list                     — list all 23 GoF patterns grouped by category
//	run_dp translate <name> <lang>  — translate to "en" or "zh"
//	run_dp case <name> <convention> — convert to PascalCase/camelCase/snake_case/...
package main

import (
	"fmt"
	"os"
	"strings"

	"reasonix/internal/designpattern/converter"
	"reasonix/internal/designpattern/matcher"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	reg := matcher.NewRegistry()
	conv := converter.New(reg)

	switch os.Args[1] {
	case "lookup":
		runLookup(reg, os.Args[2:])
	case "list":
		runList(reg)
	case "translate":
		runTranslate(conv, os.Args[2:])
	case "case":
		runCase(conv, os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage:
  run_dp lookup <name>              — resolve a pattern name
  run_dp list                       — list all GoF patterns
  run_dp translate <name> <en|zh>   — translate name
  run_dp case <name> <convention>   — convert case (e.g. snake_case, PascalCase)`)
}

func runLookup(reg *matcher.Registry, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "error: name required")
		os.Exit(1)
	}
	name := strings.Join(args, " ")
	result, err := reg.Lookup(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("English:    %s\n", result.Pattern.English)
	fmt.Printf("Chinese:    %s\n", result.Pattern.Chinese)
	fmt.Printf("Category:   %s\n", result.Pattern.Category)
	fmt.Printf("Score:      %d\n", result.Score)
	if len(result.Pattern.Aliases) > 0 {
		fmt.Printf("Aliases:    %s\n", strings.Join(result.Pattern.Aliases, ", "))
	}
}

func runList(reg *matcher.Registry) {
	byCat := reg.ListByCategory()
	for _, cat := range []string{"creational", "structural", "behavioural"} {
		patterns, ok := byCat[cat]
		if !ok {
			continue
		}
		fmt.Printf("=== %s ===\n", strings.Title(cat))
		for _, p := range patterns {
			fmt.Printf("  %-30s %s\n", p.English, p.Chinese)
		}
		fmt.Println()
	}
}

func runTranslate(conv *converter.Converter, args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "error: name and language (en|zh) required")
		os.Exit(1)
	}
	target := strings.ToLower(args[len(args)-1])
	name := strings.Join(args[:len(args)-1], " ")

	var translated string
	var err error
	switch target {
	case "en", "english":
		translated, err = conv.ToEnglish(name)
	case "zh", "chinese":
		translated, err = conv.ToChinese(name)
	default:
		fmt.Fprintf(os.Stderr, "error: unknown language %q (use en or zh)\n", target)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(translated)
}

func runCase(conv *converter.Converter, args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "error: name and convention required")
		os.Exit(1)
	}
	caseName := strings.ToLower(args[len(args)-1])
	patternName := strings.Join(args[:len(args)-1], " ")

	targetCase, err := converter.ParseCase(caseName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	result, err := conv.ConvertCase(patternName, targetCase)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(result)
}
