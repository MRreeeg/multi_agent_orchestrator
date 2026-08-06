package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client drives a retained `opencode serve` process over its loopback HTTP
// API (documented at https://opencode.ai/docs/server/).
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// NewClient creates a client for one opencode serve endpoint.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 10 * time.Minute},
	}
}

func (c *Client) post(ctx context.Context, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.HTTP.Do(req)
}

func (c *Client) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	return c.HTTP.Do(req)
}

func status(resp *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Sprintf("%d %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

// NewSession creates a session and returns its ID.
func (c *Client) NewSession(ctx context.Context, title string) (string, error) {
	payload, _ := json.Marshal(map[string]string{"title": title})
	resp, err := c.post(ctx, "/session", payload)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("opencode create session: %s", status(resp))
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.ID == "" {
		return "", fmt.Errorf("opencode create session: empty id")
	}
	return out.ID, nil
}

// Prompt sends one message and waits for the complete assistant response.
func (c *Client) Prompt(ctx context.Context, sessionID, model, prompt string) (string, error) {
	payload := map[string]any{
		"parts": []map[string]string{{"type": "text", "text": prompt}},
	}
	if model != "" {
		// opencode expects the model as an object: {providerID, modelID}.
		parts := strings.SplitN(model, "/", 2)
		if len(parts) == 2 {
			payload["model"] = map[string]string{"providerID": parts[0], "modelID": parts[1]}
		} else {
			payload["model"] = model
		}
	}
	body, _ := json.Marshal(payload)
	resp, err := c.post(ctx, "/session/"+sessionID+"/message", body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("opencode prompt: %s", status(resp))
	}
	var out struct {
		Parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	var text []string
	for _, p := range out.Parts {
		if p.Type == "text" && p.Text != "" {
			text = append(text, p.Text)
		}
	}
	return strings.TrimSpace(strings.Join(text, "")), nil
}

// Abort cancels a running turn; the session stays usable.
func (c *Client) Abort(ctx context.Context, sessionID string) error {
	resp, err := c.post(ctx, "/session/"+sessionID+"/abort", []byte(`{}`))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("opencode abort: %s", status(resp))
	}
	return nil
}

// HistoryMessage is one message from GET /session/{id}/message.
type HistoryMessage struct {
	ID   string `json:"id"`
	Role string `json:"role"`
	Text string `json:"text"`
}

// History lists the most recent messages of a session for the Runtime Console.
func (c *Client) History(ctx context.Context, sessionID string) ([]HistoryMessage, error) {
	resp, err := c.get(ctx, "/session/"+sessionID+"/message?limit=100")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("opencode history: %s", status(resp))
	}
	var raw []struct {
		Info struct {
			ID   string `json:"id"`
			Role string `json:"role"`
		} `json:"info"`
		Parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	out := make([]HistoryMessage, 0, len(raw))
	for _, m := range raw {
		var text strings.Builder
		for _, p := range m.Parts {
			if p.Type == "text" {
				text.WriteString(p.Text)
			}
		}
		out = append(out, HistoryMessage{ID: m.Info.ID, Role: m.Info.Role, Text: text.String()})
	}
	return out, nil
}
