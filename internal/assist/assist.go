// Package assist dispatches small auxiliary tasks — image analysis first and
// foremost — from a pipeline node to a vision-capable model on an
// OpenAI-compatible endpoint (default: OpenCode Zen Go route, mimo-v2.5).
//
// The pipeline side never blocks on this: the caller (an agent node) invokes it
// as a tool/command, and any failure returns a plain error string so the main
// agent can note it and continue.
package assist

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Defaults point at the OpenCode Zen Go relay (OpenAI-compatible) with mimo.
// All fields can be overridden via Options or environment variables.
const (
	DefaultEndpoint = "https://opencode.ai/zen/go/v1"
	DefaultModel    = "mimo-v2.5"
	DefaultMaxToken = 2048
	DefaultTimeout  = 90 * time.Second
)

// Options configures a single assist dispatch.
type Options struct {
	Task      string
	Images    []string // local image file paths; read and sent as data URIs
	Endpoint  string
	Model     string
	APIKey    string
	MaxTokens int
	Timeout   time.Duration
}

// Env returns Options filled from environment variables where present, keeping
// existing field values as higher priority.
func (o *Options) env() {
	if o.Endpoint == "" {
		if v := strings.TrimSpace(os.Getenv("REASONIX_ASSIST_ENDPOINT")); v != "" {
			o.Endpoint = v
		}
	}
	if o.Model == "" {
		if v := strings.TrimSpace(os.Getenv("REASONIX_ASSIST_MODEL")); v != "" {
			o.Model = v
		}
	}
	if o.APIKey == "" {
		if v := strings.TrimSpace(os.Getenv("REASONIX_ASSIST_API_KEY")); v != "" {
			o.APIKey = v
		} else if v := strings.TrimSpace(os.Getenv("OPENCODE_GO_API_KEY")); v != "" {
			o.APIKey = v
		} else if v := opencodeAuthKey(); v != "" {
			o.APIKey = v
		}
	}
	if o.MaxTokens <= 0 {
		if v := strings.TrimSpace(os.Getenv("REASONIX_ASSIST_MAX_TOKENS")); v != "" {
			var n int
			if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
				o.MaxTokens = n
			}
		}
	}
	if o.Timeout <= 0 {
		if v := strings.TrimSpace(os.Getenv("REASONIX_ASSIST_TIMEOUT")); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				o.Timeout = d
			}
		}
	}
}

func imageMime(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".bmp":
		return "image/bmp"
	default:
		return "image/png"
	}
}

// opencodeAuthKey reads the OpenCode CLI credential store so `reasonix assist`
// works with zero extra configuration on machines that already logged in to
// OpenCode (opencode-go / zen-go route). Returns "" when unavailable.
func opencodeAuthKey() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	raw, err := os.ReadFile(filepath.Join(home, ".local", "share", "opencode", "auth.json"))
	if err != nil {
		return ""
	}
	var store map[string]struct {
		Type string `json:"type"`
		Key  string `json:"key"`
	}
	if err := json.Unmarshal(raw, &store); err != nil {
		return ""
	}
	for _, name := range []string{"opencode-go", "opencode"} {
		if cred, ok := store[name]; ok && strings.TrimSpace(cred.Key) != "" {
			return strings.TrimSpace(cred.Key)
		}
	}
	return ""
}

// readImageDataURI loads a local image and returns an inline data URI.
func readImageDataURI(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", fmt.Errorf("image file is empty: %s", path)
	}
	return "data:" + imageMime(path) + ";base64," + base64.StdEncoding.EncodeToString(data), nil
}

type chatMessage struct {
	Role    string        `json:"role"`
	Content []contentPart `json:"content"`
}

type contentPart struct {
	Type     string      `json:"type"`
	Text     string      `json:"text,omitempty"`
	ImageURL *imageURL   `json:"image_url,omitempty"`
}

type imageURL struct {
	URL string `json:"url"`
}

type chatRequest struct {
	Model     string        `json:"model"`
	Messages  []chatMessage `json:"messages"`
	MaxTokens int           `json:"max_tokens,omitempty"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// Run dispatches one auxiliary task (optionally with images) and returns the
// assistant text. It never panics: configuration or API failures surface as
// errors with actionable messages.
func Run(opts Options) (string, error) {
	opts.env()
	if opts.Endpoint == "" {
		opts.Endpoint = DefaultEndpoint
	}
	if opts.Model == "" {
		opts.Model = DefaultModel
	}
	if opts.MaxTokens <= 0 {
		opts.MaxTokens = DefaultMaxToken
	}
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultTimeout
	}
	if strings.TrimSpace(opts.Task) == "" && len(opts.Images) == 0 {
		return "", fmt.Errorf("assist: empty task and no images")
	}

	content := []contentPart{{Type: "text", Text: opts.Task}}
	if len(opts.Images) > 0 {
		for _, img := range opts.Images {
			uri, err := readImageDataURI(img)
			if err != nil {
				return "", fmt.Errorf("assist: cannot read image %s: %w", img, err)
			}
			content = append(content, contentPart{Type: "image_url", ImageURL: &imageURL{URL: uri}})
		}
	}

	body, err := json.Marshal(chatRequest{
		Model:     opts.Model,
		Messages:  []chatMessage{{Role: "user", Content: content}},
		MaxTokens: opts.MaxTokens,
	})
	if err != nil {
		return "", fmt.Errorf("assist: marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(opts.Endpoint, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("assist: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if opts.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+opts.APIKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("assist: request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", fmt.Errorf("assist: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		detail := strings.TrimSpace(string(raw))
		if len(detail) > 300 {
			detail = detail[:300]
		}
		return "", fmt.Errorf("assist: %s from %s (%s)", resp.Status, opts.Endpoint, detail)
	}

	var parsed chatResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", fmt.Errorf("assist: parse response: %w", err)
	}
	if parsed.Error != nil && parsed.Error.Message != "" {
		return "", fmt.Errorf("assist: upstream error: %s", parsed.Error.Message)
	}
	if len(parsed.Choices) == 0 || parsed.Choices[0].Message.Content == "" {
		return "", fmt.Errorf("assist: empty completion from %s", opts.Model)
	}
	return parsed.Choices[0].Message.Content, nil
}
