// Command orchestrator-app runs the multi-agent steward console as a native
// desktop app window (WebView2) instead of a browser tab. The HTTP server is
// embedded in-process on a loopback random port; the window is the only
// surface the user interacts with.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"

	webview "github.com/webview/webview_go"

	"reasonix/internal/config"
	"reasonix/internal/control"
	"reasonix/internal/serve"
)

// user32 helpers for window position + icon (no extra dependencies).
var (
	user32               = syscall.NewLazyDLL("user32.dll")
	procMoveWindow       = user32.NewProc("MoveWindow")
	procGetWindowRect    = user32.NewProc("GetWindowRect")
	procGetSystemMetrics = user32.NewProc("GetSystemMetrics")
)

type winRect struct{ Left, Top, Right, Bottom int32 }

func windowPosPath() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "orchestrator-app", "window.json")
}

func saveWindowPos(hwnd uintptr) {
	path := windowPosPath()
	if path == "" {
		return
	}
	var r winRect
	if ret, _, _ := procGetWindowRect.Call(hwnd, uintptr(unsafe.Pointer(&r))); ret == 0 {
		return
	}
	pos := map[string]int{
		"x": int(r.Left), "y": int(r.Top),
		"w": int(r.Right - r.Left), "h": int(r.Bottom - r.Top),
	}
	data, _ := json.Marshal(pos)
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	_ = os.WriteFile(path, data, 0o644)
}

func restoreWindowPos(hwnd uintptr, defW, defH int) {
	path := windowPosPath()
	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			var p struct {
				X, Y, W, H int
			}
			if json.Unmarshal(data, &p) == nil && p.W > 0 && p.H > 0 {
				procMoveWindow.Call(hwnd, uintptr(p.X), uintptr(p.Y), uintptr(p.W), uintptr(p.H), 1)
				return
			}
		}
	}
	// Center on the primary screen.
	sw, _, _ := procGetSystemMetrics.Call(0)
	sh, _, _ := procGetSystemMetrics.Call(1)
	x := int((int32(sw) - int32(defW)) / 2)
	y := int((int32(sh) - int32(defH)) / 2)
	if x < 0 {
		x = 0
	}
	if y < 0 {
		y = 0
	}
	procMoveWindow.Call(hwnd, uintptr(x), uintptr(y), uintptr(defW), uintptr(defH), 1)
}

func main() {
	// Share history with the browser start scripts (start-orchestrator.ps1 sets
	// REASONIX_ORCHESTRATOR_DATA_DIR). The desktop binary lives in <repo>/bin,
	// so default the persistence root to <repo>/.data/orchestrator unless the
	// environment already points somewhere else.
	if strings.TrimSpace(os.Getenv("REASONIX_ORCHESTRATOR_DATA_DIR")) == "" {
		if exe, err := os.Executable(); err == nil {
			root := filepath.Clean(filepath.Join(filepath.Dir(exe), "..", ".data", "orchestrator"))
			_ = os.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", root)
		}
	}

	bc := serve.NewBroadcaster()
	ctrl := control.New(control.Options{Sink: bc})
	defer ctrl.Close()

	var serveCfg config.ServeConfig
	if path := config.UserConfigPath(); path != "" {
		if cfg := config.LoadForEdit(path); cfg != nil {
			serveCfg = cfg.Serve
		}
	}
	srv := serve.New(ctrl, bc, serveCfg)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	server := &http.Server{Handler: srv.Handler()}
	go func() { _ = server.Serve(ln) }()

	const defW, defH = 1440, 900
	w := webview.New(false)
	defer w.Destroy()
	hwnd := uintptr(w.Window())
	// Restore the last window position (or center it) BEFORE Run so the
	// bottom input bar is on screen without dragging on every launch.
	w.SetSize(defW, defH, webview.HintNone)
	restoreWindowPos(hwnd, defW, defH)
	w.SetTitle("多智能体管家 · 多Agent 编排控制台")
	w.Navigate(fmt.Sprintf("http://127.0.0.1:%d/orchestrator", port))
	w.Run()
	saveWindowPos(hwnd)

	_ = server.Shutdown(context.Background())
}
