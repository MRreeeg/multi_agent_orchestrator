package serve

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"reasonix/internal/assist"
)

func TestBuildHistoryTextInjectsVision(t *testing.T) {
	dir := imageAttachmentDir()
	os.MkdirAll(dir, 0755)
	id := "999_visiontest"
	os.WriteFile(filepath.Join(dir, id+".png"), tinyPNGBytes, 0644)
	orig := analyzeVision
	analyzeVision = func(o assist.Options) (string, error) {
		return "设计稿：登录表单", nil
	}
	defer func() { analyzeVision = orig }()

	images := []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}{{ID: id, Name: "login.png"}}
	got := buildHistoryTextWithImages("[用户]: 做登录页", images)
	if !strings.Contains(got, "[图片 login.png]: 设计稿：登录表单") {
		t.Fatalf("missing vision injection, got:\n%s", got)
	}
}

func TestBuildHistoryTextVisionFailureDegrades(t *testing.T) {
	orig := analyzeVision
	analyzeVision = func(o assist.Options) (string, error) {
		return "", fmt.Errorf("vision api down")
	}
	defer func() { analyzeVision = orig }()

	images := []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}{{ID: "missing", Name: "a.png"}}
	got := buildHistoryTextWithImages("[用户]: x", images)
	if !strings.Contains(got, "[图片 a.png 无法解析：vision api down]") {
		t.Fatalf("missing degrade injection, got:\n%s", got)
	}
	if !strings.Contains(got, "[用户]: x") {
		t.Fatal("history text must survive vision failure")
	}
}
