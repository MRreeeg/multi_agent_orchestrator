# 聊天输入框图片附件 + 需求分析视觉上下文（L-54）设计

## 背景与目标

用户在编排器主界面聊天输入框发送需求文字，由 Flash（`/requirements/analyze`）生成流水线编排。现在希望：

1. 图片可以通过**拖拽**或**`+` 按钮选择**进入输入框；
2. 图片作为附件，**随文字需求一起**提交需求分析；
3. 图片由视觉模型转述后注入分析 prompt，让 Flash"看到"图（UI 截图 / 设计稿 / 报错图）；
4. 图片缩略图展示在对话消息里，并**随会话持久化**，刷新可回看。

## 现状与可复用资产

- 需求分析 `analyzeRequirement`（`internal/serve/orchestrator.go:1275`）：接收
  `{text, history, pipelineInfo, executor, model, agent}`，经 `reasonix` 子进程
  （stdin 传 prompt）产出编排 JSON。**无图片通道**。
- 视觉通道 `internal/assist.Run(opts)`：`Options{Task, Images []string（本地路径）}`
  → 把图片读为 data URI 发给 vision 模型（默认 mimo-v2.5 / opencode zen-go 路由，
  env 可配）→ 返回文本描述。**可直接复用**，节点辅助手（assist）已在用。
- 会话消息持久化：`OrchestrationSession.ChatMessages / RequirementMessages`
  （`[]ChatMsg{role,text,meta}`）经 orchestrator session API 保存/加载。

## 架构（方案 A：后端落盘 + assist 视觉转述注入）

```
拖拽/加号选图 ──► POST /upload-image ──► 落盘 dataDir/attachments/<fileID>.<ext>
   │                                      └── 返回 {id, name, url}
   ▼
待发送附件区（缩略图 chip，可删除）
   │  发送
   ▼
sendChat：图片 → 用户消息（ChatMsg.Images 持久化）＋ analyze body.images
   ▼
后端 analyzeRequirement：
   ├── 对 images 逐张 assist.Run 视觉转述 → 文本
   ├── 转述文本注入 historyText（[图片 <name>]: <描述>）
   └── 正常 Flash 分析生成编排
   ▼
对话消息渲染 <img src=/orchestrator/api/images/<fileID>>，刷新后回读
```

### 组件

1. **前端输入区附件（index.html）**
   - composer 内新增 `+` 按钮 `#chat-attach-btn`（输入框左侧），触发隐藏的
     `<input type=file id=chat-file-input accept="image/*" multiple>`。
   - composer 整条监听 `dragover`（preventDefault + 高亮 class）与 `drop`
     （`e.dataTransfer.files` 过滤图片）。
   - 附件待发送区 `#chat-attachments`（composer 上方）：每个 chip 显示缩略图
     （上传返回的 URL）+ 文件名 + × 删除按钮。
   - `state.pendingAttachments: [{id, name, url, path}]`。选图后立即
     `POST /upload-image`，成功才入列；失败 toast 并丢弃。
   - `sendChat` 发送时：把 `pendingAttachments` 并入用户消息（持久化）与
     `analyze` 请求，清空待发送区。发送中禁止再传图。
   - `addChatMessage(role, text, meta, persist, images)` 渲染图片缩略图。
   - 刷新加载历史：`ChatMsg.Images` 渲染回显（复用同一 URL）。

2. **后端上传/回读（serve）**
   - `POST /orchestrator/api/upload-image`：JSON `{data: <base64>, name}`。
     校验：MIME 白名单 `image/png|jpeg|gif|webp|bmp`；大小上限 10MB；
     base64 解码 + 魔数校验（用 `http.DetectContentType`，与扩展名一致才收）。
     落盘 `dataDir/attachments/<unixms>_<rand>.<ext>`（不依赖 sessionID，因为
     sendChat 里 session 是分析前才创建的）。返回
     `{id, name, url: "/orchestrator/api/images/<fileID>"}`。
   - `GET /orchestrator/api/images/<fileID>`：按扩展名回 Content-Type 读文件。

3. **需求分析扩展（serve）**
   - body 增加 `images: [{id, name, path}]`（本次发送图片；path 由后端从
     attachments 目录按 id 定位，前端不传磁盘路径）。
   - 分析前对每张图调 `assist.Run(Options{Task: <转述指令>, Images: [path]})`，
     结果注入 `historyText`：`[图片 <name>]: <转述描述>`。转述失败注入
     `[图片 <name> 无法解析：<err>]`，**不阻塞**分析。
   - 历史消息中的图片：前端构造 `history` 时把该消息的 `images` 一并收集进
     `images` 数组，后端统一转述（保持上下文一致；图片数量少，成本可控）。
   - 视觉调用注入变量 `analyzeVision = assist.Run`，便于单测替换。
   - 分析模型选择不受影响（默认仍是 deepseek-flash 文本模型）。

4. **持久化（orchestrator 包）**
   - `ChatMsg` 增加 `Images []ChatImage`；新增
     `type ChatImage struct{ ID, Name string }`（Path 不入 JSON，服务端按
     `dataDir/attachments/<ID>.<ext>` 定位，避免前端接触磁盘路径）。

### 数据流

1. 拖入/选图 → `upload-image` 落盘 → 附件 chip 显示。
2. 发送：`addChatMessage('user', text, '', true, images)` →
   `requirementMessages` 含 `Images` → session API 持久化。
3. `analyzeRequirement`：视觉转述 → 注入 prompt → Flash 生成编排。
4. 历史回看：消息 `Images` → `<img src=/orchestrator/api/images/<id>>`。

### 错误处理

| 场景 | 处理 |
|---|---|
| 拖入/选择非图片 | toast 拒绝，不入列 |
| 上传失败 / 超限 / 魔数不符 | toast 报错，不入列 |
| 发送中再传图 | 忽略并提示 |
| 视觉转述失败 | 注入 `[图片 无法解析]` 占位，继续分析 |
| 图片文件被删 | 回读 404，前端占位图标 |
| 分析失败 | 沿用既有重试（strict prompt 重试一次）与报错路径 |

### 测试

- **serve 包**
  - `upload-image`：合法 PNG 返回 id/url；非图片（text 魔数）拒绝；超 10MB 拒绝。
  - `GET images/<id>`：返回正确 Content-Type；未知 id 404。
  - `analyzeRequirement` 带 images：mock `analyzeVision` 返回固定文本，断言
    prompt/historyText 注入；转述失败断言注入占位且分析继续。
- **orchestrator 包**
  - `ChatMsg` 含 `Images` 的会话保存/加载 round-trip。
- **前端**：node --check（内联脚本提取）；手工验证拖拽/选择/删除/回显。

## 范围

- 不做：图片多轮对话中的按需读取工具（assist 机制已覆盖节点侧）；图片 OCR；
  附件管理 UI（删除清理会话图片文件）；上传进度条（图片小，本地快）。
- 视觉模型默认 mimo-v2.5（assist 默认），不新增选择 UI（env 可配，够用）。