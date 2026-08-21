#!/usr/bin/env bash
# 一键启动 Multi-Agent Orchestrator 控制台（macOS / Linux）。
# 与 scripts/start-orchestrator.ps1 对应：
#   - 默认监听 127.0.0.1:8788，数据存 <仓库根目录>/.data/orchestrator
#   - bin 下已有二进制且比源码新则复用，否则重新编译
# 用法:
#   ./scripts/start-orchestrator.sh                # 前台运行，Ctrl+C 停止
#   ./scripts/start-orchestrator.sh -p 8789 -n     # 换端口、不开浏览器
#   ./scripts/start-orchestrator.sh -d /tmp/mao-data -w /path/to/workspace
set -euo pipefail

PORT=8788
NO_BROWSER=0
DATA_DIR=""
WORKSPACE_DIR=""

usage() {
  echo "用法: $0 [-p PORT] [-n] [-d DATA_DIR] [-w WORKSPACE_DIR]"
  echo "  -p  监听端口（默认 8788）"
  echo "  -n  不自动打开浏览器"
  echo "  -d  数据目录（默认 <仓库根目录>/.data/orchestrator）"
  echo "  -w  工作区目录（默认仓库根目录）"
  exit 1
}

while getopts "p:nd:w:h" opt; do
  case "$opt" in
    p) PORT="$OPTARG" ;;
    n) NO_BROWSER=1 ;;
    d) DATA_DIR="$OPTARG" ;;
    w) WORKSPACE_DIR="$OPTARG" ;;
    *) usage ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ADDR="127.0.0.1:${PORT}"
BASE_URL="http://${ADDR}"
ORCH_URL="${BASE_URL}/orchestrator"

command -v go >/dev/null 2>&1 || { echo "未找到 Go。请安装 Go 1.25 或更高版本并加入 PATH。" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "未找到 curl，健康检查需要它。" >&2; exit 1; }

# 入口二进制不固定：优先 orchestrator-app，其次 reasonix（与 PowerShell 脚本一致）
ENTRY_NAME="reasonix"
if [[ -x "${REPO_ROOT}/bin/orchestrator-app" || -d "${REPO_ROOT}/cmd/orchestrator-app" ]]; then
  ENTRY_NAME="orchestrator-app"
fi
ENTRY_BIN="${REPO_ROOT}/bin/${ENTRY_NAME}"

DATA_DIR="${DATA_DIR:-${REPO_ROOT}/.data/orchestrator}"
mkdir -p "${DATA_DIR}"
export REASONIX_ORCHESTRATOR_DATA_DIR="${DATA_DIR}"
WORKSPACE_DIR="${WORKSPACE_DIR:-${REPO_ROOT}}"

echo "Multi-Agent Orchestrator"
echo "  project:    ${REPO_ROOT}"
echo "  workspace:  ${WORKSPACE_DIR}"
echo "  persistence:${DATA_DIR}"
echo "  address:    ${ORCH_URL}"

# 复用已有二进制；源码有更新或二进制缺失时重新编译
NEED_BUILD=0
if [[ ! -x "${ENTRY_BIN}" ]]; then
  NEED_BUILD=1
elif [[ -n "$(find "${REPO_ROOT}/internal" "${REPO_ROOT}/cmd" "${REPO_ROOT}/go.mod" -name '*.go' -newer "${ENTRY_BIN}" 2>/dev/null | head -n1 || true)" ]]; then
  NEED_BUILD=1
fi
if [[ "${NEED_BUILD}" == "1" ]]; then
  echo "编译当前源码 (cmd/${ENTRY_NAME})..."
  (cd "${REPO_ROOT}" && go build -o "${ENTRY_BIN}" "./cmd/${ENTRY_NAME}")
else
  echo "复用已有二进制: ${ENTRY_BIN}"
fi

echo "启动服务..."
(cd "${WORKSPACE_DIR}" && exec "${ENTRY_BIN}" serve --addr "${ADDR}" --auth none) &
PROC_PID=$!
trap 'kill "${PROC_PID}" 2>/dev/null || true' EXIT INT TERM

READY=0
for _ in $(seq 1 60); do
  sleep 0.5
  if curl -fsS "${BASE_URL}/status" >/dev/null 2>&1; then READY=1; break; fi
  kill -0 "${PROC_PID}" 2>/dev/null || break
done

if [[ "${READY}" != "1" ]]; then
  echo "服务启动失败。请直接运行排查: (cd \"\$(pwd)\" && go run ./cmd/${ENTRY_NAME} serve --addr ${ADDR} --auth none)" >&2
  exit 1
fi

echo "服务已就绪 (PID ${PROC_PID})"
# 开启工具自动审批（YOLO），失败不影响使用
curl -fsS -X POST "${BASE_URL}/auto-approve-tools" \
  -H 'Content-Type: application/json' -d '{"on":true}' >/dev/null 2>&1 || true

if [[ "${NO_BROWSER}" != "1" ]]; then
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "${ORCH_URL}" >/dev/null 2>&1 || true
  elif command -v open >/dev/null 2>&1; then
    open "${ORCH_URL}" >/dev/null 2>&1 || true
  fi
fi

echo "控制台: ${ORCH_URL}"
echo "按 Ctrl+C 停止服务。"
wait "${PROC_PID}" || true
