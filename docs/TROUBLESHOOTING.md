# 问题记录 / Troubleshooting

本文件记录开发与运行中遇到的关键问题、根因、修复方式，供后续排查参考。

## 2026-08-20：编排控制台页面卡在"正在加载编排控制台…"（前端 TDZ 顶层崩溃）

### 现象

- 打开 `http://127.0.0.1:8788/orchestrator` 后，页面永远停留在"正在加载编排控制台…"遮罩。
- 浏览器 **零 API 请求**（服务端 access log 只有 `/orchestrator` 与 `/favicon.ico`）。
- 服务端所有 API 单独/并发 curl 均正常（毫秒级返回），服务端无死锁。
- `document.readyState` 为 `complete`，但 `typeof init === 'undefined'`，`typeof $ === 'undefined'`。
- 手动调用 `init()` 报 `ReferenceError: Cannot access '$' before initialization`（或 script 顶层直接中断）。

### 根因

`index.html` 主脚本顶层：

```js
const state = {
  nodes: [
    { ..., assist: { enabled: true, model: defaultAssistModel(), ... } },  // ← 在 state 初始化表达式内调用
    ...
  ],
  ...
};
```

- `defaultAssistModel()` 是**函数声明**（会提升，可调用），但其函数体第一行是 `(state.nodeTypes || [])`——**读取正在初始化（TDZ）的 `const state`**。
- `const`/`let` 存在**暂时性死区（TDZ）**：绑定已创建但未初始化时访问即抛 `ReferenceError`。
- 该错误发生在 script 顶层 → **整个 script 中断**：`$`（L1604）、`init()`（L6437）、`DOMContentLoaded` 注册（文件末行）全部未执行 → init 永不运行 → boot 遮罩永不揭开、零 API 请求。

> 关键认知：**函数提升 ≠ 函数体内引用的变量已可用**。在对象字面量初始化表达式内调用一个"读全局 state"的函数是天然陷阱——函数定义处（L5700）在 `const state`（L1316）之后，语法正确、`node --check` 通过，但运行时顶层必崩。

### 排查弯路（引以为戒）

- 先怀疑服务端：并发 curl 全通排除死锁。
- headless Edge `--dump-dom` + `--virtual-time-budget` 是**假阳性**来源（虚拟时间不等真实网络，页面"看似没加载完"）。
- CDP 真实等待（`Runtime.evaluate` + `awaitPromise`）才拿到真实证据：`boot:""`（未揭）、init 未注册。
- 分文件二分：`git stash` 全量改动 → 旧版正常；只还原前端 → 正常；确认问题 100% 在前端。
- 最终靠 **eval 手动调 `init()` 捕获完整异常**（`e.stack`）拿到 `ReferenceError`，再用 **Node `vm` mock DOM 复现顶层执行** 确认崩溃点在 state 初始化。

### 修复

1. `defaultAssistModel()` 改为**不访问 state**：返回独立变量 `assistDefaultModel`（`var`，默认 `'opencode/mimo-v2.5-free'`）。
2. 原探测逻辑抽到 `refreshAssistDefaultModel()`（可安全读 `state.nodeTypes`），在 **`loadNodeTypes()` 成功后**调用填充 `assistDefaultModel`。
3. 模板节点 / `normalizeNode` / `createNode` / `renderNodes` 中的调用点不变（全部安全）。

### 预防规则

- **顶层对象字面量初始化表达式内，禁止调用任何可能读取同文件顶层 `const`/`let` 的函数**（TDZ 崩溃）。
- 若函数可能在"全局绑定尚未就绪"时被调用（如模板/默认值计算），函数体内**不要直接引用全局绑定**，改为注入/独立变量/延迟初始化。
- 前端 inline script 改动后，除 `node --check` 语法检查外，**用 Node `vm` 跑一遍顶层执行**（mock `document`/`window`/`localStorage`/`fetch` 等最小环境），能抓出 TDZ 类运行时顶层崩溃。

### 相关文件

- `internal/serve/orchestrator_frontend/index.html`：`defaultAssistModel` / `refreshAssistDefaultModel`（L5700 附近）、`state` 定义（L1316 附近）、`loadNodeTypes`（L5930 附近）。