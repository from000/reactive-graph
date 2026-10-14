# ⚡ ReactiveGraph

**一个反应式图执行引擎**——编译期构建依赖图，运行期只执行受影响的节点。

它不是又一个"每次 `invoke` 全量重跑"的智能体框架，而是把**选择性执行**当作
核心抽象：声明 `reads`/`writes`，引擎自动决定哪些任务需要重跑、哪些可以跳过。

[![CI](https://github.com/from000/reactive-graph/actions/workflows/ci.yml/badge.svg)](https://github.com/from000/reactive-graph/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-from000.github.io%2Freactive--graph-2f6f4f)](https://from000.github.io/reactive-graph/)
[![Status: v0.1.0 alpha](https://img.shields.io/badge/status-v0.1.0%20alpha-orange)](RELEASE_CHECKLIST.md)

**English:** [README.en.md](README.en.md)

---

## 为什么值得看

| 痛点 | LangGraph 的代价 | ReactiveGraph 的做法 |
|---|---|---|
| 每次调用全量重跑 | 1000 节点改 10 个输入 → 跑 1000/1000 | 依赖分析 + 缓存失效 → **只跑 10/1000** |
| 选择性执行难写 | 要逐节点手写 `key_func`，共享 state 时缓存全失效 | 声明 `reads` 即自动生效 |
| 调试靠猜 | 调度决策不透明 | 因果 trace 有序可导出，审计不用猜 |

> 数字来自 2026-10-12 实测（Python `DriverHost` 经 RGP/1 + Node 子进程，
> median of 3）：同样 1000 节点、10 个受影响，langgraph 用其最优配置
> （手写 `key_func` + 同线程复用）约 **2278 ms**，ReactiveGraph 约 **257 ms**。
> 完整方法、no-gain 场景与 O(n²) 修复见 [`docs/competitive-proof.md`](docs/competitive-proof.md)。

> **真实 LLM 上的省钱证据**（Agnes，逐次计数的实测，不是估算）：
> 同一份文档做确定性抽取，用户只改了无关的 `tone` 后重跑——
> LangGraph 调用 `llm_extract` **2 次**，ReactiveGraph **1 次**（指纹命中跳过），
> 总调用 4 vs 3，tokens 2165 vs 1661。见
> [`benchmarks/savings/REPORT.md`](benchmarks/savings/REPORT.md)。
> **限定**：只有声明 `kind="pure"` + `reads` 的确定性子任务才省；
> agent 主链（model/tools 是 effect）**不省**，首次运行也不省。

---

## 核心能力

- **⚡ 选择性执行** — 事件路由决定 effect 任务资格；computed 按 read-set 失效；
  pure 任务输入指纹不变即跳过；effect 任务 receipt 幂等，确认即不重放。
- **📦 事务性状态** — `transaction` 携带读写集 + patches 原子提交，冲突有明确策略
  （reducer / priority / 串行），不是"全量重算后合并"。
- **💾 持久化执行** — durable event log（SQLite / 内存）+ checkpoint + 恢复，
  重启后从日志精确回放。
- **🔎 因果 trace** — 每个调度决策（start/done/retry/skip/invalidate）有序可导出。
- **🔌 流式与中断** — `stream` 值流 / 自定义流；`interrupt` / `resume` 支持
  human-in-the-loop。
- **🧩 scope 子状态** — 命名子状态命名空间，天然模块化。

---

## 快速开始

> **安装状态**：当前通过 GitHub 源码分发，尚未上 PyPI / npm。
> `pip install reactivegraph` 与 `npm install @reactivegraph/sdk-js` 暂不可用，
> 请用下面的命令。

Python（**必须一次装全**——`reactivechain` / `reactivegraph-sdk` /
`reactivegraph-cli` 依赖 `reactivegraph>=0.1.0`，单独装会解析失败）：

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

> **装不上？** `SSL_ERROR_SYSCALL` / 超时通常是本机 `~/.gitconfig` 的
> `url.<base>.insteadOf` / `http.proxy` 拦截了 GitHub，与仓库无关。换 SSH 或
> 临时清代理：
>
> ```bash
> pip install "git+ssh://git@github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph"
> # 或
> GIT_CONFIG_GLOBAL=/dev/null pip install "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph"
> ```

TypeScript 端从源码构建（npm 无法直接消费 monorepo 的 `workspace:*`）：

```bash
git clone https://github.com/from000/reactive-graph.git
cd reactive-graph && pnpm install && pnpm -r build
```

### 3 行 Hello World

```python
from reactivegraph import ReactiveGraph

def build(b):
    b.task("greet", kind="effect", fn=lambda i: {"msg": f"hi {i['name']}"},
           on=("visit",))

g = ReactiveGraph.build(build)
print(g.invoke("visit", {"name": "Ada"})["msg"])   # "hi Ada"
```

TypeScript 对象式 API（`packages/sdk-js`）：

```ts
import { ReactiveGraph, invoke } from "@reactivegraph/sdk-js";

const graph = ReactiveGraph.build((b) => {
  b.task({
    id: "greet", kind: "effect",
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

---

## 真实项目证明：DeerFlow

我们把 [DeerFlow](https://github.com/bytedance/deer-flow) 的后端执行底座迁移到
ReactiveGraph，保留产品逻辑做真实对照（命令、结果、边界见
`benchmarks/deerflow_real/REPORT.md`）。CI 实测（CI 拉真实 postgres:16 /
redis:7，本地缺服务时跳过同样数量的集成用例）：

```text
ReactiveGraph core:      931 passed (CI, real Postgres + Redis)
ReactiveChain:           221 passed
DeerFlow backend:        19,101 passed, 82 skipped, 0 failed（分片合计）
生产路径对照:            输出一致；延迟 ~2.3x 更低，并发吞吐 ~4x 更高
SQLite 恢复:             checkpoint 与 store 都能跨进程重启存活
```

> **数字口径**：生产路径（`create_deerflow_agent`，含中间件链）是**该引用的数字**
> ——延迟 ~2.3x、并发吞吐 ~4x。更早的 7.19x/8.07x 是窄口径
> （`create_agent` 直接调用、不含中间件链），见 `docs/competitive-proof.md` §1b。

---

## 仓库布局

```
packages/                 TS：protocol / driver / sdk-js / devtools-protocol / testkit
python/                   Python：reactivegraph / reactivegraph_sdk / reactivegraph_cli
benchmarks/               benchmark harness 与 workloads
docs/                     规范、基准、性能对比
```

| 命令 | 用途 |
|---|---|
| `make test` | Python 原生测试 |
| `pnpm typecheck` / `pnpm test` | 全部 TypeScript 包 |
| `pnpm benchmark` | 选择性更新基准 |

---

## 了解更多

- **[基准与复现](docs/benchmarks.md)** — 选择性更新 + 真实 upstream langgraph 差分测量，含 no-gain 场景的诚实记录
- **[商业评估](docs/commercial-readiness.md)** — 定位、边界与落地评估
- **[FAQ](docs/faq.md)** — 包括"为什么还能看到 LangChain"
- **[安全](SECURITY.md)** — gateway 认证 / 字段级权限 / 状态脱敏 / 只读 importer

## 贡献与试点

见 [`CONTRIBUTING.md`](CONTRIBUTING.md) 与 [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)。
欢迎 issue、PR 与 [Discussions](https://github.com/from000/reactive-graph/discussions)。

我们正在寻找少量团队在真实 agent / workflow 负载上试用 v0.1.0
（工具调用、重试、持久状态、人工审核、多租户网关，或从 state-graph 迁移）。
见 [`.github/PILOT_INVITATION.md`](.github/PILOT_INVITATION.md)，感兴趣请开
["Pilot user" issue](https://github.com/from000/reactive-graph/issues/new?template=pilot_user.md)。

---

## License

MIT —— 见 [`LICENSE`](LICENSE)。

## 第三方声明

兼容层（`messages` / `tools` / `ToolNode` / `channels` / `middleware` /
`checkpoint` / `store` / `message_utils` 等）接口与行为适配自
`langchain-ai/langgraph` 与 `langchain-ai/langchain`（MIT）；`uuid6` 生成实现
改编自 `oittaa/uuid6-python`（MIT）。完整版权声明、参考版本与许可证全文见
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md)。

## 提交历史说明

公开仓库以单次 squash commit 发布，`docs/` 中引用的 commit hash 指向私有开发
检查点，**在本仓库中解析不到**。fork 修订以 `<current> (was <pre-rewrite>)`
标注（`ab0737f`（was `57e8d2d`）、`606187b`（was `3b55c36`））；更早的
`c128e42` 是 2026-09-27 checkout，其记录的测量后来在 `606187b` 与 `ab0737f`
上复现。`d2aca13` / `f9f3127` 是普通 upstream commit；LangGraph pins
（`11ee1859…`）属于 `langchain-ai/langgraph`。
