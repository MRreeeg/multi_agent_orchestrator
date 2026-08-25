package orchestrator

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// 辅助手委派历史持久化。
//
// 背景：AssistDispatch 每次委派都是 fresh 会话，运行时（opencodeRuntimeMgr）
// 与控制台状态仅存于进程内存——Orchestrator 退出即全部蒸发，重启后无法回看
// "上次退出前辅助手做了什么、结论是什么"。本文件把每次委派（成功与失败）
// 追加写入 DataRoot()/assist_history.jsonl，并提供按时间倒序的读取接口；
// serve 层经 GET /orch-assist/history 暴露，前端重启后即可恢复展示。

// AssistHistoryRecord 是一次辅助手委派的落盘记录。
type AssistHistoryRecord struct {
	Time      string   `json:"time"`                // RFC3339
	Task      string   `json:"task"`                // 任务文本（截断至 2000 rune）
	Images    []string `json:"images,omitempty"`    // 图片绝对路径
	ModelRef  string   `json:"modelRef"`            // provider/model
	Driver    string   `json:"driver,omitempty"`
	OK        bool     `json:"ok"`
	Result    string   `json:"result,omitempty"`    // 成功时的完整结果文本
	Error     string   `json:"error,omitempty"`     // 失败原因
	SessionID string   `json:"sessionID,omitempty"`
	RuntimeID string   `json:"runtimeID,omitempty"`
}

const (
	assistHistoryFile  = "assist_history.jsonl"
	assistTaskMaxRune  = 2000
	assistResultMaxRune = 20000 // 结果全文保留过大时截断，防单条记录膨胀
)

var assistHistoryMu sync.Mutex

func assistHistoryPath() string {
	return filepath.Join(DataRoot(), assistHistoryFile)
}

// truncateRunes 按 rune 截断并加省略标记。
func truncateRunes(s string, max int) string {
	rs := []rune(strings.TrimSpace(s))
	if len(rs) <= max {
		return strings.TrimSpace(s)
	}
	return string(rs[:max]) + "…（已截断）"
}

// appendAssistHistory 追加一条委派记录；失败仅返回 error 不 panic，
// 持久化故障不得影响委派主链路。
func appendAssistHistory(rec AssistHistoryRecord) error {
	if rec.Time == "" {
		rec.Time = time.Now().Format(time.RFC3339)
	}
	rec.Task = truncateRunes(rec.Task, assistTaskMaxRune)
	if len(rec.Result) > 0 || rec.OK {
		rec.Result = truncateRunes(rec.Result, assistResultMaxRune)
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	assistHistoryMu.Lock()
	defer assistHistoryMu.Unlock()
	if err := os.MkdirAll(DataRoot(), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(assistHistoryPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(line, '\n'))
	return err
}

// AssistHistory 返回最近 limit 条委派记录（时间倒序）；limit<=0 取默认 50。
// 文件不存在视为无历史（首次启动），返回空切片而非错误。
func (s *Store) AssistHistory(limit int) ([]AssistHistoryRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	assistHistoryMu.Lock()
	defer assistHistoryMu.Unlock()
	f, err := os.Open(assistHistoryPath())
	if err != nil {
		if os.IsNotExist(err) {
			return []AssistHistoryRecord{}, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []AssistHistoryRecord
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 单行上限 1MB（结果可能很长）
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec AssistHistoryRecord
		if json.Unmarshal([]byte(line), &rec) == nil {
			out = append(out, rec)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	// 时间倒序：文件本身是追加序（旧→新），取尾部再反转。
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}
