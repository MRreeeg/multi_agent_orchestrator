// Command orchestrator-app runs the multi-agent steward console as a native
// desktop app window (WebView2) instead of a browser tab. The HTTP server is
// embedded in-process on a loopback random port; the window is the only
// surface the user interacts with.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"

	webview "github.com/webview/webview_go"

	"reasonix/internal/config"
	"reasonix/internal/control"
	"reasonix/internal/serve"
)

func main() {
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

	w := webview.New(false)
	defer w.Destroy()
	w.SetTitle("多智能体管家 · 多Agent 编排控制台")
	w.SetSize(1440, 900, webview.HintNone)
	w.Navigate(fmt.Sprintf("http://127.0.0.1:%d/orchestrator", port))
	w.Run()

	_ = server.Shutdown(context.Background())
}
