package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"reasonix/internal/assist"
)

// assistCommand dispatches a small auxiliary task (image analysis first) to a
// vision-capable model. Usage:
//
//	<binary> assist "describe this screenshot" [--image a.png] [--image b.jpg] [--model mimo-v2.5] [--driver opencode|claude]
//
// Options come from flags, then environment (REASONIX_ASSIST_*), then defaults
// (OpenCode Zen Go route, mimo-v2.5).
func assistCommand(args []string) int {
	var task string
	var images []string
	var model, driver string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--image" || args[i] == "-i":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "assist: --image requires a path")
				return 2
			}
			i++
			images = append(images, args[i])
		case args[i] == "--model" || args[i] == "-m":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "assist: --model requires a value")
				return 2
			}
			i++
			model = args[i]
		case args[i] == "--driver" || args[i] == "-d":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "assist: --driver requires opencode or claude")
				return 2
			}
			i++
			driver = args[i]
		case strings.HasPrefix(args[i], "-"):
			fmt.Fprintf(os.Stderr, "assist: unknown flag %s\n", args[i])
			return 2
		default:
			if task != "" {
				task += " " + args[i]
			} else {
				task = args[i]
			}
		}
	}
	if strings.TrimSpace(task) == "" && len(images) == 0 {
		fmt.Fprintf(os.Stderr, "assist: usage: %s assist \"task\" [--image path ...] [--model name] [--driver opencode|claude]\n", filepath.Base(os.Args[0]))
		return 2
	}

	result, err := assist.Run(assist.Options{Task: task, Images: images, Model: model, Driver: driver})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println(result)
	return 0
}
