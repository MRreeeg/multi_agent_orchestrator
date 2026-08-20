package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"reasonix/internal/event"
)

// 辅助手（Helper Agent）委派协议。
//
// 辅助手是 Orchestrator 托管的独立 opencode serve 运行时，运行视觉模型
// （默认 opencode/mimo-v2.5；driver=claude 时用 claude 模型）。流水线执行者
// 节点（模型可能无视觉能力）在遇到识图等任务时，通过
// POST /orchestrator/api/orch-assist/dispatch 委派：请求携带任务文本 + 图片
// 绝对路径（与 orchestrator 同机共享文件系统），辅助手用 read 工具读取图片，
// 一次 Prompt 直接产出分析文本返回。
//
// 每次委派使用独立会话（fresh）；运行时按 (model, workspace) 复用，运行状态
// 与控制台经既有 opencode 运行时机制暴露（SSE 事件 + /runtimes/<id>/console）。
// 任何失败都返回可读错误，主 agent 据此如实声明，禁止编造。

// assistRuntimeNodePrefix 固定前缀使辅助手运行时与节点运行时在 key 维度隔离。
const assistRuntimeNodePrefix = "__assist__"

// AssistDispatchOptions 配置一次辅助手委派。
type AssistDispatchOptions struct {
	Task    string        // 识图/小任务描述
	Images  []string      // 图片绝对路径（与 orchestrator 同机）
	Model   string        // 视觉模型名；空按 driver 取默认
	Driver  string        // "opencode"（默认）| "claude"
	Timeout time.Duration // 单次委派超时；默认 90s，上限 300s
}

// AssistDispatchResult 是一次成功委派的结果。
type AssistDispatchResult struct {
	RuntimeID string
	Port      int
	ModelRef  string
	Result    string
	SessionID string
}

// assistDenyTools 辅助手只读工具集：可读文件（含图片），不可写/执行/联网。
func assistDenyTools() map[string]bool {
	return map[string]bool{
		"bash": false, "edit": false, "write": false, "move": false,
		"patch": false, "create": false, "delete": false,
		"task": false, "webfetch": false, "websearch": false, "mcp__*": false,
	}
}

// assistDisciplinePrompt 是辅助手系统提示：只读 + 用 read 读图 + 输出即交付物。
const assistDisciplinePrompt = `You are the Helper Agent in an automated multi-agent pipeline, specialized in vision tasks (reading screenshots, designs, error images) and small side tasks. Your final response text IS the deliverable: it is returned verbatim to the calling agent node.

Rules:
1. Read every image file listed in the prompt with the read tool (it supports images). Never claim to have seen an image you did not actually read.
2. If an image cannot be read, say so plainly and describe only what you determined from text — never fabricate pixel-level details.
3. Answer directly and completely in one pass. No plan narration, no restating the task.
4. Output only the deliverable content (the analysis text). No preamble or trailing commentary.`

// assistPromptText 组装辅助手输入：图片路径清单 + 识图任务。
func assistPromptText(task string, images []string) string {
	var b strings.Builder
	b.WriteString("你是辅助手（Helper Agent），负责识图等小任务。请用 read 工具读取以下图片文件（绝对路径），然后完成识图任务：\n")
	for _, img := range images {
		b.WriteString("- " + img + "\n")
	}
	b.WriteString("\n识图任务：\n")
	b.WriteString(task)
	return b.String()
}

// assistModelRef 由 driver+model 拼 opencode 模型引用（provider/modelID）。
func assistModelRef(driver, model string) string {
	driver = strings.ToLower(strings.TrimSpace(driver))
	if driver == "" {
		driver = "opencode"
	}
	if strings.TrimSpace(model) == "" {
		if driver == "claude" {
			model = "claude-sonnet-4-6"
		} else {
			model = "mimo-v2.5"
		}
	}
	return driver + "/" + model
}

// SetOrchestratorAddr 设置编排服务对外地址（host:port），注入辅助手委派协议
// （执行者 curl 目标）。由 serve 启动时调用；为空时辅助手提示降级为如实声明。
func (s *Store) SetOrchestratorAddr(addr string) {
	s.orchAddr.Store(strings.TrimSpace(addr))
}

// AssistDispatch 执行一次辅助手委派。事件（assist_start/assist_done）经 Store
// 的 emitter 广播，前端据此在 run-dash 显示状态并提供控制台入口。
func (s *Store) AssistDispatch(ctx context.Context, opts AssistDispatchOptions) (*AssistDispatchResult, error) {
	if strings.TrimSpace(opts.Task) == "" && len(opts.Images) == 0 {
		return nil, fmt.Errorf("assist: empty task and no images")
	}
	for _, img := range opts.Images {
		if info, err := os.Stat(img); err != nil || info.IsDir() {
			return nil, fmt.Errorf("assist: image path not readable: %s", img)
		}
	}
	driver := strings.ToLower(strings.TrimSpace(opts.Driver))
	if driver == "" {
		driver = "opencode"
	}
	modelRef := assistModelRef(driver, opts.Model)
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	if timeout > 300*time.Second {
		timeout = 300 * time.Second
	}

	spec := ExecSpec{
		Workspace:    detectWorkspace(),
		ModelRef:     modelRef,
		NodeID:       assistRuntimeNodePrefix + "|" + modelRef,
		NodeLabel:    "辅助手 " + modelRef,
		ApprovalMode: "auto",
		Mode:         "serve",
	}
	rt, err := opencodeRuntimeMgr.ensure(ctx, spec, nil)
	if err != nil {
		return nil, fmt.Errorf("assist: start helper runtime: %w", err)
	}
	s.emitAssistEvent(event.AssistStart, rt, modelRef, opts.Task, false, "")

	client, err := opencodeRuntimeMgr.reserveTurn(rt)
	if err != nil {
		s.emitAssistEvent(event.AssistDone, rt, modelRef, opts.Task, false, err.Error())
		return nil, fmt.Errorf("assist: helper busy: %w", err)
	}
	dispatchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	sessionID, err := opencodeRuntimeMgr.createSession(dispatchCtx, rt, client, spec)
	if err != nil {
		opencodeRuntimeMgr.finishTurn(rt, sessionID, err)
		s.emitAssistEvent(event.AssistDone, rt, modelRef, opts.Task, false, err.Error())
		return nil, fmt.Errorf("assist: create helper session: %w", err)
	}
	text, promptErr := client.Prompt(dispatchCtx, sessionID, modelRef, assistDisciplinePrompt, assistPromptText(opts.Task, opts.Images), assistDenyTools())
	opencodeRuntimeMgr.finishTurn(rt, sessionID, promptErr)
	if promptErr != nil {
		s.emitAssistEvent(event.AssistDone, rt, modelRef, opts.Task, false, promptErr.Error())
		return nil, fmt.Errorf("assist: helper turn failed: %w", promptErr)
	}
	if strings.TrimSpace(text) == "" {
		msg := "assist: helper returned empty completion"
		s.emitAssistEvent(event.AssistDone, rt, modelRef, opts.Task, false, msg)
		return nil, fmt.Errorf("%s", msg)
	}
	result := &AssistDispatchResult{
		RuntimeID: rt.ID,
		Port:      rt.Port,
		ModelRef:  modelRef,
		Result:    text,
		SessionID: sessionID,
	}
	s.emitAssistEvent(event.AssistDone, rt, modelRef, opts.Task, true, "")
	return result, nil
}

// emitAssistEvent 广播辅助手委派事件（Text=任务摘要，Detail=JSON）。
func (s *Store) emitAssistEvent(kind event.Kind, rt *opencodeRuntime, modelRef, task string, ok bool, errMsg string) {
	detail, _ := json.Marshal(map[string]any{
		"runtimeID":   rt.ID,
		"port":        rt.Port,
		"model":       modelRef,
		"ok":          ok,
		"error":       errMsg,
		"taskPreview": truncateRune(strings.TrimSpace(task), 120),
	})
	text := strings.TrimSpace(task)
	if len([]rune(text)) > 120 {
		text = string([]rune(text)[:120]) + "…"
	}
	s.emit(event.Event{Kind: kind, Text: text, Detail: string(detail)})
}

// ── 自动识图兜底（AutoVision）──
//
// 依赖 LLM 自觉调用委派协议不可靠（无视觉模型可能忽略提示、没有 curl 或
// 不知道图片绝对路径）。Orchestrator 因此在流水线层主动拦截：run 携带上传
// 图片清单（id+原名），executor 任务文本含图片引用时：
//  1. 无论模型，先注入附件图片的绝对路径清单（视觉模型可用 read 工具直读，
//     无视觉模型委派时也有了可靠路径）；
//  2. assist 开启且模型无视觉时，Orchestrator 自动调用辅助手运行时完成识图，
//     把结构化结果直接注入任务文本——executor 不再需要自己调 curl。

// modelSupportsVision 报告模型是否具备图像输入能力（经 opencode read 工具
// 可读图）。未知/未指定模型保守视为无视觉，交给辅助手兜底。
func modelSupportsVision(model string) bool {
	m := strings.ToLower(strings.TrimSpace(model))
	if m == "" {
		return false
	}
	if strings.Contains(m, "mimo-v2.5") && !strings.Contains(m, "pro") {
		return true
	}
	if strings.HasPrefix(m, "glm") || strings.HasPrefix(m, "claude") {
		return true
	}
	return false
}

// attachmentDir 返回上传图片附件目录（与 serve 的 imageAttachmentDir 一致）。
func attachmentDir() string {
	return filepath.Join(DataRoot(), "attachments")
}

// imageIDRe/validImageRef 校验附件 id（UnixMilli "_" hex4），与 serve 侧
// validImageID 同构；id 会拼入 filepath.Glob，glob 元字符必须被拒绝。
var imageIDRe = regexp.MustCompile(`^[0-9]{13}_[0-9a-f]{8}$`)

func validImageRef(id string) bool { return imageIDRe.MatchString(id) }

// imageAttachmentPaths 把 run 的图片附件解析为磁盘绝对路径（按声明顺序，
// 跳过已删除/非法文件）。
func imageAttachmentPaths(refs []ImageRef) []string {
	var out []string
	for _, ref := range refs {
		if !validImageRef(ref.ID) {
			continue
		}
		matches, err := filepath.Glob(filepath.Join(attachmentDir(), ref.ID+".*"))
		if err != nil || len(matches) == 0 {
			continue
		}
		out = append(out, matches[0])
	}
	return out
}

// taskReferencesImages 判定任务文本是否引用图片（按附件原名/ID 或图片扩展名
// 文件名模式）。宽松匹配避免漏掉架构师的不同写法。
func taskReferencesImages(task string, refs []ImageRef) bool {
	task = strings.TrimSpace(task)
	if task == "" {
		return false
	}
	for _, ref := range refs {
		if ref.Name != "" && strings.Contains(task, ref.Name) {
			return true
		}
		if ref.ID != "" && strings.Contains(task, ref.ID) {
			return true
		}
	}
	return imageFileRe.MatchString(task)
}

// imageFileRe 匹配常见图片文件名引用（中文/字母/数字/空格/点 + 扩展名）。
var imageFileRe = regexp.MustCompile(`(?i)[\p{L}\p{N}\-_. ]+\.(png|jpe?g|gif|webp|bmp)\b`)

// autoVisionDispatch 执行自动识图委派；测试可替换为假实现避免启动真实运行时。
var autoVisionDispatch = func(ctx context.Context, s *Store, opts AssistDispatchOptions) (*AssistDispatchResult, error) {
	return s.AssistDispatch(ctx, opts)
}

// autoVisionInject 是自动识图兜底入口：解析 run 图片附件，注入路径清单，
// 需要时自动委派辅助手识图并注入结果。永不阻塞流水线——委派失败降级为
// 如实声明。
func (s *Store) autoVisionInject(ctx context.Context, run *PipelineRun, node *AgentNode, task string) string {
	if len(run.Images) == 0 || strings.TrimSpace(task) == "" {
		return task
	}
	paths := imageAttachmentPaths(run.Images)
	if len(paths) == 0 {
		return task
	}
	if !taskReferencesImages(task, run.Images) {
		return task
	}

	var b strings.Builder
	b.WriteString("\n\n## Orchestrator 附件图片（本机绝对路径，与编排服务同机文件系统）\n")
	for i, ref := range run.Images {
		p := ""
		if i < len(paths) {
			p = paths[i]
		} else if !validImageRef(ref.ID) {
			continue
		} else if m, err := filepath.Glob(filepath.Join(attachmentDir(), ref.ID+".*")); err == nil && len(m) > 0 {
			p = m[0]
		}
		name := ref.Name
		if name == "" {
			name = ref.ID
		}
		if p == "" {
			b.WriteString(fmt.Sprintf("- %s → (文件缺失，无法定位)\n", name))
		} else {
			b.WriteString(fmt.Sprintf("- %s → %s\n", name, p))
		}
	}

	// 仅对无视觉模型自动委派；有视觉的模型直接 read 上面注入的路径即可。
	assistOn := node != nil && node.Assist != nil && node.Assist.Enabled
	if !assistOn || modelSupportsVision(node.Model) || node.Type == NodeArchitect {
		return task + b.String()
	}

	// 委派：任务文本给辅助手作上下文（截断），由它逐张读图并输出。
	visionTask := strings.TrimSpace(task)
	runes := []rune(visionTask)
	if len(runes) > 3000 {
		visionTask = string(runes[:3000]) + "\n…（任务文本过长已截断，按上述要求识图）"
	}
	res, err := autoVisionDispatch(ctx, s, AssistDispatchOptions{
		Task:    visionTask,
		Images:  paths,
		Timeout: 150 * time.Second,
	})
	if err != nil {
		b.WriteString("\n## 辅助手自动识图失败（Orchestrator 已尝试委派视觉运行时）\n" +
			"失败原因：" + err.Error() + "\n" +
			"如本任务确需识图，请如实声明「无法识图」，禁止编造任何图像内容。\n")
		return task + b.String()
	}
	b.WriteString("\n## 辅助手自动识图结果（Orchestrator 已完成识图，直接使用以下内容，禁止再调用任何识图工具或委派命令）\n")
	b.WriteString(res.Result)
	return task + b.String()
}