package cli

import (
	"fmt"
	"os"
	"strings"

	"reasonix/internal/assist"
)

// assistCommand dispatches a small auxiliary task (image analysis first) to a
// vision-capable model. Usage:
//
//	reasonix assist "describe this screenshot" [--image a.png] [--image b.jpg]
//
// Options come from flags, then environment (REASONIX_ASSIST_*), then defaults
// (OpenCode Zen Go route, mimo-v2.5).
func assistCommand(args []string) int {
	var task string
	var images []string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--image" || args[i] == "-i":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "assist: --image requires a path")
				return 2
			}
			i++
			images = append(images, args[i])
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
		fmt.Fprintln(os.Stderr, "assist: usage: reasonix assist \"task\" [--image path ...]")
		return 2
	}

	result, err := assist.Run(assist.Options{Task: task, Images: images})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println(result)
	return 0
}
