# ReactiveGraph

**反应式图执行引擎**——以响应式更新为内核的智能体/工作流运行时。上游对标
`langchain-ai/langgraph`,但执行模型不同:我们不做"每次全量重跑",
而是编译期建依赖图、运行期只执行受影响的任务。

[![CI](https://github.com/from000/reactive-graph/actions/workflows/ci.yml/badge.svg)](https://github.com/from000/reactive-graph/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-from000.github.io%2Freactive--graph-2f6f4f)](https://from000.github.io/reactive-graph/)
[![Status: v0.1.0 alpha](https://img.shields.io/badge/status-v0.1.0%20alpha-orange)](RELEASE_CHECKLIST.md)

**English:** [README.en.md](README.en.md)

> **History and commit references.** The public repository was published as a
> single squashed commit, so commit hashes quoted in `docs/` identify private
> development checkpoints and do **not** resolve here. Fork revisions are
> quoted as `<current> (was <pre-rewrite>)` where the fork's author identity was
> rewritten: `ab0737f` (was `57e8d2d`) and `606187b` (was `3b55c36`). An older
> proof document cites `c128e42`, a 2026-09-27 checkout the rewrite dropped; the
> measurements it records were reproduced afterwards on `606187b` and
> `ab0737f`. `d2aca13` and `f9f3127` are ordinary upstream commits; the
> LangGraph pins (`11ee1859…`) belong to `langchain-ai/langgraph`.

## 为什么是反应式

LangGraph 的 Pregel 在每次 `invoke` 时全量执行所有节点；ReactiveGraph 依赖
分析 + 缓存失效，只执行受影响的节点。同一确定性图、同一机器：

| 场景 | upstream langgraph | ReactiveGraph |
|---|---|---|
| 对话图 invoke（warm 中位） | ~1.5 ms | ~0.004 ms |
| 1000 节点 fan-out 改 1 字段 | ~11602 ms | ~15.3 ms |
| 1000 节点 / 10 受影响，改 k0 | ~4669 ms | ~6.9 ms |
| **no-gain：1000 节点全依赖链** | **~6428 ms** | **~8.7 ms** |

> ⚠️ 上表数字来自早期一次未入库的临时本机测量，**仅供量级参考，
> 不可复现**。权威数字以 `benchmarks/differential/`（真实 langgraph 1.2.11 vs
> 原生 Driver，同图同输入、输出一致断言 + median）为准，见
> `benchmarks/differential/README.md`。

## 核心能力

* **选择性执行** — 事件路由决定 effect 任务资格；computed 按 read-set 失效；
  pure 任务输入指纹不变即跳过；effect 任务 receipt 幂等，确认即不重放。
* **事务性状态** — `transaction` 携带读写集 + patches 原子提交，冲突有明确
  策略（reducer / priority / 串行），不是"全量重算后合并"。
* **computed 缓存** — selector 声明 read-set，只读集变化才重算。
* **scope 子状态** — 命名子状态命名空间，天然模块化。
* **持久化执行** — durable event log（SQLite/内存）+ checkpoint + 恢复，
  重启后从日志精确回放。
* **因果 trace** — 调度每个决策（start/done/retry/skip/invalidate）有序
  可导出，调试与审计不用猜。
* **流式与中断** — `stream` 值流/自定义流；`interrupt`/`resume` 支持
  human-in-the-loop。

## 快速开始

> **安装状态**：本项目当前通过 GitHub 源码分发，尚未发布到 PyPI / npm。
> `pip install reactivegraph` 与 `npm install @reactivegraph/sdk-js`
> 在包上架前无法使用 —— 请使用下面的安装命令。

Python 端（**必须一次装全**：`reactivechain` / `reactivegraph-sdk` /
`reactivegraph-cli` 依赖 `reactivegraph>=0.1.0`，单独安装会在解析依赖时失败）：

```bash
pip install \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph" \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivechain" \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph_sdk" \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph_cli"
```

只装核心引擎：

```bash
pip install "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph"
```

安装可选 extra（需要 pip >= 24；旧版 pip 不支持 PEP 508 extras 与 git
子目录的组合，请先 `python -m pip install -U pip`）：

```bash
pip install "reactivegraph[langchain-ecosystem] @ \
  git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph"
```

TypeScript 端需要从源码构建（npm 无法直接消费本仓库 monorepo 的
`workspace:*` 依赖）：

```bash
git clone https://github.com/from000/reactive-graph.git
cd reactive-graph && pnpm install && pnpm -r build
# 然后在你的项目里引用 packages/sdk-js（例如 pnpm add file:../reactive-graph/packages/sdk-js）
```

```python
from reactivegraph import ReactiveGraph

def build(b):
    b.task("greet", kind="effect", fn=lambda i: {"msg": f"hi {i['name']}"},
           on=("visit",))          # 或 b.on("visit", "greet")

g = ReactiveGraph.build(build)
out = g.invoke("visit", {"name": "Ada"})   # 实例方法，返回 state 字典
print(out["msg"])  # "hi Ada"
```

TypeScript 原生 API（`packages/sdk-js`）使用对象式签名：

```ts
import { ReactiveGraph, invoke } from "@reactivegraph/sdk-js";

const graph = ReactiveGraph.build((b) => {
  b.task({
    id: "greet",
    kind: "effect",
    handler: (input) => ({
      reads: ["name"], writes: ["msg"],
      patches: [{ path: ["msg"], operation: "set", value: `hi ${(input as { name: string }).name}` }],
    }),
  });
  b.on("visit", "greet");
});
const out = await invoke(graph, "visit", { name: "Ada" });
// out.state.msg === "hi Ada"
```

## 命令

| 命令 | 用途 |
|---|---|
| `make test` | Python 原生测试 |
| `pnpm typecheck` / `pnpm test` | TS 全部包 |
| `uv run --directory python/reactivegraph pytest -q` | Python 原生 |
| `pnpm benchmark` | 选择性更新基准 |

## 仓库布局

```
packages/                 TS：protocol / driver / sdk-js / devtools-protocol / testkit
python/                   Python：reactivegraph / reactivegraph_sdk / reactivegraph_cli
benchmarks/               benchmark harness 与 workloads
docs/                     规范、基准、性能对比
```

## 真实项目证明

我们将 [DeerFlow](https://github.com/bytedance/deer-flow) 的后端执行底座迁移到
ReactiveGraph，并保留其产品逻辑进行真实对照。结果、命令和边界见
`benchmarks/deerflow_real/REPORT.md`。

以下为 CI 实测（CI 会拉起真实 postgres:16 / redis:7 服务，本地缺服务时会跳过同样
数量的集成用例）：

```text
ReactiveGraph core:      822 passed, 71 skipped   (CI，含真实 Postgres + Redis)
ReactiveChain:           221 passed
DeerFlow backend:        19,101 passed, 82 skipped, 0 failed（分片合计）
real full-factory dual:  output parity, 7.19x first-run / 8.07x repeat speed-up
SQLite recovery:         checkpoint and store both survive process restart
```

商业评估入口见 `docs/commercial-readiness.md`。
常见问题（包括“为什么还能看到 LangChain”）见 `docs/faq.md`。

## 基准

性能数据与复现方法见 `docs/benchmarks.md`（选择性更新 + 与真实 upstream
langgraph 的差分测量，含 no-gain 场景的诚实记录）。

## 安全

见 `SECURITY.md`（gateway 认证 / 字段级权限 / 状态脱敏 / 只读 importer）。

## 开源贡献

见 `CONTRIBUTING.md` 与 `CODE_OF_CONDUCT.md`。我们欢迎 issue 与 PR。

## License

MIT —— 见 `LICENSE`。

## 第三方声明

兼容层（`messages` / `tools` / `ToolNode` / `channels` / `middleware` /
`checkpoint` / `store` / `message_utils` 等）在接口与行为上适配自
`langchain-ai/langgraph` 与 `langchain-ai/langchain`（MIT）；`uuid6` 生成实现
改编自 `oittaa/uuid6-python`（MIT）。完整的版权声明、参考版本与许可证全文见
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md)。
