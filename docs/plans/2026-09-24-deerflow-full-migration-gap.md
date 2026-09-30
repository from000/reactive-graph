# DeerFlow 全面替换为 ReactiveGraph — 差距量化

> **Historical snapshot:** the measurements below are the 2026-09-24 to
> 2026-09-29 working record. See `docs/plan-completion-audit.md` for the
> current 2026-09-30 reconciliation and `docs/proof/README.md` for the
> frozen proof bundle.
>
> Commit 引用说明：正文中的短 hash 是本仓库压缩前的私有开发检查点（公开仓库为
> 单 commit），DeerFlow 侧 hash 属于独立的 `deer-flow-reactive` 检出。
>
> 数据来源：fork `$DEERFLOW_REACTIVE_ROOT`（branch
> `reactivegraph-checkpointer`，upstream base `d2aca13`）现场统计，可复核。
> 2026-09-30 补充：fork 当前 HEAD 为 `ab0737f`（身份重写后与原 `57e8d2d`
> 树相同），19,098 项后端离线全量结果记录于 `606187b`（原 `3b55c36`），
> 299 项前端 E2E 通过；
> ReactiveGraph native PostgreSQL checkpoint/store 16 项、fork PostgreSQL
> retention 60 项、其余 268 passed/3 skipped、DeerFlow Redis Streams bridge
> 7 项与 Driver RedisCache 3 项均有真实服务证据。生产规模、故障负载和完整
> 产品等价仍未证明。

## 结论先行

**分两层看结论：DeerFlow fork 的后端执行底座已经完成大规模替换并通过
19,098 项离线全量验证；真实 full-factory 输出与 upstream 归一化一致，
本机观测到 7-8 倍延迟优势，SQLite checkpoint/store 均已证明跨进程恢复，
原生 PostgreSQL checkpoint/store、PostgreSQL 扩展套件与两条 Redis 路径也已在真实服务上验证。
但前端/channels、MCP/sandbox 的生产规模与故障负载仍需专项证明，
public release 与外部 pilot 也尚未完成。**

- ✅ **已证明**：真实 gateway replay 从修复前 55 帧收敛到 committed golden
  的 15 帧，事件序列精确相等，且 replay miss 为空。真实 lead-agent 路径也能在
  langgraph 被封杀时导入。真实 run worker 的 delta rollback 与 delta resume
  linearization 已有端到端测试证据。
- ✅ **新增证明**：同输入 full-factory 双跑，fork 输出与 upstream 归一化一致；
  首次调用约 7.19 倍、重复调用约 8.07 倍本机延迟优势；SQLite checkpoint/store
  均通过独立写入进程退出后的恢复测试。
- ✅ **新增证明**：ReactiveGraph native PostgreSQL checkpoint/store 通过
  16 项集成测试；DeerFlow fork PostgreSQL retention 60 项、其余 268 passed/
  3 skipped；Driver RedisCache 3 项与 DeerFlow Redis Streams bridge 7 项通过
  真实 Redis 7.2 验证；goal continuation 有 26 项 worker 测试；`custom`
  stream mode 已进入支持矩阵并有事件去重测试。
- 🟡 **边界更新**：前端 E2E 已在 `606187b`（原 `3b55c36`）上 299 passed，
  当前 `ab0737f`（原 `57e8d2d`，树相同）只修 asyncpg 测试 DSN；channels、
  MCP/sandbox 的生产规模与故障负载、重启风暴/内存上限/真实 slow-consumer
  背压仍未证明；因此不应扩大为
  "整个产品所有子系统已完成或全面超过 LangGraph/LangChain"。

按不同口径衡量（均为 2026-09-26 实测）：

| 口径 | 覆盖 | 占比 |
|---|---:|---:|
| Harness 源码文件（590 个 / 143,186 LOC） | 非 core 直连 LC/LG 仅 12 处 / 7 文件 | 导入层接近清零 |
| `langchain_core` 直连（174 处 / 125 文件） | 作为 host 兼容边界有意保留 | 非迁移目标 |
| 非 core `langchain`（1 处 / 1 文件） | deermem 的 `init_chat_model` 惰性调用 | 接近清零 |
| `langgraph`（11 处 / 6 文件） | 均为 provider、类型检查或 fallback 惰性路径 | lead-agent 路径 0 依赖 |
| 运行时相关 upstream 测试函数 | 见下方专项批次；完整生产语义仍未覆盖 | 进行中 |

**真实 replay golden（2026-09-26）：**

```text
$ cd backend
$ PYTHONPATH="$PWD:$PWD/packages/reactivegraph" \
    .venv/bin/python -m pytest tests/test_replay_golden.py -q \
    -p no:randomly --tb=short -rf
1 passed in 16.84s

# committed golden 本身：metadata + 13 values + end = 15 帧
$ jq '.events | length' tests/fixtures/replay/write_read_file.ultra.events.json
15

# 修复前 got=55, want=15；replay misses 非空。
# 修复后 got == want，且 replay misses 为空。
```

根因不是 replay fixture 漂移，而是 fallback 对每个 task 无条件发 `values`。
LangGraph 只在 super-step 提交 public channel write 后发 `values`；空更新或
仅 `__reactivegraph_*` / `jump_to` 等 private 更新只应留下 task span。修复后
`create_agent` 的真实 no-op middleware 路径不再伪造 public state snapshot。

**Core 回归证据：**

```text
python/reactivegraph 全量：802 passed, 12 skipped in 135.47s
```

**DeerFlow 回归证据：**

```text
真实 replay golden：1 passed in 16.84s
真实 run worker rollback：124 passed in 5.32s
gateway checkpoint/recovery + worker delivery/delta resume/rollback +
delta channel checkpointers/state：208 passed, 4 skipped in 12.38s
assistant replay + replay golden/provider + run event stream + stream bridge +
subgraph namespace：159 passed in 13.13s
```

backend 全量（排除要求 Docker Engine 28+ 的
`test_aio_sandbox_local_backend.py`）结果为：

```text
19098 passed, 168 skipped, 21 deselected, 50 warnings in 2805.92s
```

这次运行修复了两类测试环境问题：

- 本地 loopback Git/MCP/OpenViking mock 被系统代理 7892 劫持，测试内已显式
  `NO_PROXY=127.0.0.1,localhost`；
- MCP notification release 测试缺少后台任务同步点，已等待 `release_started`。

最终 `make test` 是全绿离线结果，无产品失败。

**Full-factory 双跑（2026-09-27）：**

| 指标 | upstream | ReactiveGraph fork |
|---|---:|---:|
| 引擎 | LangGraph Pregel | `reactivegraph.create_agent.AgentGraph` |
| 首次调用 | 6.975 ms | 0.971 ms |
| 重复调用 | 3.765 ms | 0.466 ms |
| 重复输出稳定 | yes | yes |
| 归一化结果一致 | yes | yes |

非语义差异仅包括：upstream 在 AI message 上设置 graph name；ReactiveGraph
在 `additional_kwargs` 中重复保存 `invalid_tool_calls`。移除这两个表示层字段
后，完整 state 输出一致。

**跨进程持久化恢复（2026-09-27）：**

- `test_sqlite_recovers_after_writer_process_exit`：进程 1 写 checkpoint 后退出，
  进程 2 读取同一 checkpoint 与 metadata。
- `test_sqlite_store_recovers_after_writer_process_exit`：进程 1 写 store item
  后退出，进程 2 读取并继续更新。

**Live 状态（2026-09-27 最终）：**

原始批量运行为：

```text
42 passed, 4 failed in 3463.33s
```

当时 4 个失败均为外部模型端点连接错误：`ark-cn-beijing.bytedance.net` 走 7892
与直连均超时；备用 `www.tokenforus.org` TLS 失败，`tokenforus.org` 返回 521。

随后使用当前可用且支持 chat/tool-call 的 OpenAI-compatible 网关复验，原失败
4 个用例全部通过：

```text
test_client_e2e.py::TestToolCallFlow::test_tool_call_produces_events
  1 passed

test_create_deerflow_agent_live.py::test_minimal_agent_responds
  1 passed

test_create_deerflow_agent_live.py::test_agent_with_custom_tool
test_create_deerflow_agent_live.py::test_features_mode_middleware_chain
  2 passed
```

`test_client_e2e` 的测试配置已显式注册真实 `bash` tool；同文件离线回归结果为
`32 passed, 11 deselected`。live gate 已完成。

**Delta rollback 关键证据：**

根因是 `ThreadCheckpointer._checkpoint_for()` 无条件持久化 folded values，
违反 DeltaChannel 协议。现在 `update_state` raw writes 通过 `delta_writes`
提交；非 snapshot Delta raw write 写到 parent checkpoint 的 `put_writes`，
child checkpoint 省略 non-snapshot Delta `channel_values`。达到
`snapshot_frequency` 或 5000 supersteps 时才写 snapshot，host saver 使用
LangGraph 自己的 `_DeltaSnapshot` namedtuple。该证据证明真实 run worker 的
rollback 与 delta resume linearization 可用，但**不等于**所有 durable provider
和完整 fork/rollback 生产语义均已证明。

**关键新增证据（可复核）：**

```text
# 临时探针脚本未入库；下列历史计数来自锁定 harness 快照。
# 470/470 harness 模块在 langgraph 被封杀时全部导入成功
$ cd backend/packages/harness && ../../.venv/bin/python "${TMPDIR:-/tmp}/harness_sweep.py"
{"total": 470, "ok": 470, "fail": 0}

# 真实 lead agent 模块在 langgraph blocked 下导入成功
$ cd backend/packages/harness && ../../.venv/bin/python "${TMPDIR:-/tmp}/leadprobe.py"
OK deerflow.agents.lead_agent.agent
```

说明：`langgraph` 残留的 11 处 import 全在 runtime provider 的**惰性分支**里
（sqlite/postgres store & checkpointer、`TYPE_CHECKING` 下的 `CompiledStateGraph`），
不在 import 期执行，所以全量模块扫描与真实 agent 导入都能通过。

## 1. Harness 规模与直连依赖分布

```text
packages/harness/deerflow  590 files / 143,186 LOC
其中非 core 直连 LC/LG import  12 occurrences / 7 files
  langchain_core.*         174 occurrences / 125 files （host 兼容边界，保留）
  langchain.*（非 core）      1 occurrence / 1 file
  langgraph.*               11 occurrences / 6 files（全部惰性/仅 TYPE_CHECKING）
```

| 子系统 | 状态 |
|---|---|
| agents / middleware | 🟢 真实 lead-agent 路径已在 langgraph blocked 下导入；todo / summarization / store 已换成本引擎实现 |
| tools | 🟡 `langchain.tools` 已全部清零（37 文件），改用 `langchain_core.tools` / `reactivegraph.tools` |
| models | 🟡 非 core `langchain.chat_models` 仅剩 deermem 的 1 处延迟调用；provider 分支保留 |
| runtime checkpointer/store | 🟢 内存、SQLite、PostgreSQL provider 均为原生实现；Driver RedisCache 与 DeerFlow Redis Streams bridge 已实现；delta rollback/resume 关键路径已换 |
| persistence / community / config / skills / sandbox / subagents / mcp / extensions | ❌ 尚未完成行为等价替换 |

## 2. 运行时层细项（我们唯一真正进入的层）

| 上游模块 | 上游测试函数 | 我们覆盖的行为 | 状态 |
|---|---:|---|---|
| `stream_bridge/memory.py` | 58 | live subscribe / replay / heartbeat / gap / END / cleanup | 🟡 核心行为 |
| `events/store/memory.py` | 90 | put / put_batch / seq / list_events | 🟡 最小子集 |
| `journal.py` (1300 LOC) | 116 | 缓冲批量写 / run.start / run.end / run.delivery / completion data | 🟡 约 5% |
| `runs/worker.py` (约 2900 LOC) | ~150 | 元数据帧 / 流式发布 / 取消 / 终态 / 错误帧 / end | 🟡 约 6% |
| `runs/manager.py` | 74 | create / get / set_status / create_or_reject | 🟡 约 8% |
| `runs/store/*` | 69 | put/get/list/state/completion/tokens/cancel/lease/takeover/atomic admission | 🟡 约 25% |
| `checkpointer/*` | 56+ | 内存 saver 契约子集（get_tuple/list/put/put_writes/delete_thread + async 镜像、parent chain、pending writes、ns/thread 隔离、provider 单例）；Delta raw write/snapshot cadence/rollback 关键路径已实装；sqlite/postgres 原生 provider 已实装并在真实服务上验证；Driver RedisCache 与 DeerFlow Redis Streams bridge 另行验证；剩余 backend 仍 fail closed | 🟡 约 45% |
| rollback / ownership | 205 | durable reservation、CancelOutcome、lease heartbeat、孤儿 reconciliation、worker id、retry-after；真实 delta rollback 与 delta resume linearization 已通过；完整 fork/全 provider rollback 未证明 | 🟡 约 30% |
| goal continuation | 41 | goal worker 26 项测试（含 continuation、satisfied、clarification、token budget） | 🟢 |
| `custom` stream mode | — | `_SUPPORTED_STREAM_MODES` + custom event 单次投递测试 | 🟢 |

## 3. 仍未完整替换的层（体量远大于已做部分）

```text
backend/app            127 Python files    （gateway / 路由 / SSE 端点）
backend/packages       632 Python files
frontend               843 TS/TSX/JS/JSX files
```

- 后端 gateway 关键路径：🟡 已有 checkpoint/recovery、delivery 与 rollback 实装和测试证据；
  channels、前端与完整生产语义仍未全面证明
- sandbox、MCP、skills、persistence：🟡 部分路径已进入 import/适配层，但未达到行为等价
- `custom` stream mode：🟢 已实现；Driver RedisCache：🟢 真实 Redis 已验证；
  channels / cross-process bridge 的生产语义仍需专项验证
- tools / models / subagents：**已进入 import 层**（`langchain.tools` 清零、
  `langchain.chat_models` 仅剩 deermem 的 1 处延迟调用），但行为替换未完成
- 多 worker ownership / lease / reconciliation：**已进入**；delta rollback/resume
  已验证，完整 fork/rollback 与所有 durable provider 仍未完成

## 4. 阶段划分与估算

| 阶段 | 内容 | 状态 |
|---|---|---|
| P1 | factory 基线 + middleware 子集 + stream modes | ✅ 已完成 |
| P2 | runtime worker / bridge / journal / store | ✅ 核心路径完成；全量离线 19,098 passed |
| P3 | runtime 生产语义：durable store、rollback、ownership、goal、`custom` | 🟡 SQLite/Postgres、delta rollback/ownership、goal、`custom` 完成；完整 fork 与全 provider 等价仍开放 |
| P4 | tools / models / MCP / subagents / sandbox / skills | 🟡 生产路径已切换；规模负载等价待测 |
| P5 | gateway / channels / SSE / 前端 | 🟡 后端 gateway 关键路径已有证据；前端 299 项 E2E 已通过；channels 生产语义待验证 |

**当前证据下：后端执行底座与核心生产语义已完成并通过全量离线验证；P3 的
SQLite/PostgreSQL durable provider、delta rollback/resume、ownership、
goal continuation 与 `custom` 已完成；完整 fork/rollback 与所有 provider
等价仍未逐条证明。P4/P5 的产品级负载与 channels 等价性仍待专项验证；
前端 E2E 已完成一次 CI-equivalent 全量复跑。
整体已从早期 PoC 进入“核心底座可发布验证，产品边界待补”阶段。**

## 5. 怎么验证（每个阶段的可复核判据）

```bash
# 真实上游对照：同一输入双跑，先断言输出一致再比性能
REPO_ROOT="$PWD" uv run --directory python/reactivechain \
  pytest "$PWD/benchmarks/deerflow_real/" -q

# 逐层回归
REPO_ROOT="$PWD" uv run --directory python/reactivegraph python -m pytest -q
uv run --directory python/reactivechain python -m pytest -q
pnpm --dir packages/driver test
uvx ruff check python/reactivegraph python/reactivechain benchmarks/deerflow_real
python scripts/check-docs.py
git diff --check
```

阶段验收必须同时满足：

1. 不 import LangChain/LangGraph（用 grep 断言）
2. 上游对应测试区域的语义逐条对照，未实现的 fail closed
3. 与真实 upstream 双跑输出一致（不是只跑我们自己的测试）
4. 未实现的 backend 必须抛错，禁止静默降级为内存（sqlite/postgres/redis
   现已原生实现；其余 backend 仍 fail closed）
5. 无法达成一致的用例如实记为 ❌，不得静默近似

## 6. 复现统计的命令

```bash
cd "$DEERFLOW_REACTIVE_ROOT/backend"
# 直连依赖分布
grep -rE '^\s*(from|import)\s+(langchain|langgraph)\b' packages/harness/deerflow --include='*.py' | wc -l
# 运行时测试规模
python3 - <<'PY'
import re, pathlib
deftest = re.compile(r'^\s*(?:async\s+)?def\s+test_')
targets = ['runtime','run_','stream','journal','checkpoint','worker','bridge','cancel']
tot = 0
for p in pathlib.Path('tests').rglob('test_*.py'):
    if any(t in str(p) for t in targets):
        tot += sum(1 for l in p.read_text().splitlines() if deftest.match(l))
print(tot)
PY
```

## 7. 类型重指向的耦合实测（2026-09-24 修正）

早期计划假设“结构相同的类型可以直接把 DeerFlow import 指到 ReactiveGraph”。
实测证明这是**假等价（false parity）**：上游引擎与工具节点对类型做 `isinstance` 判定，
结构相同不满足身份判定，会静默走错分支或直接崩溃。

实测证据（当时使用两个临时探针脚本，对照 venv 内真实上游源码；探针脚本
未入库，下列计数为 2026-09-24 的历史快照）：

| 符号 | langgraph 内 isinstance/except/issubclass 位点 | langchain 内位点 |
|---|---:|---:|
| Command | 21 | — |
| Runtime | 20 | — |
| Send | 14 | — |
| GraphBubbleUp | 12 | 4 |
| Interrupt | 6 | — |
| END | 2 | — |
| Overwrite | 1 | — |

代表性反例：

1. 把原生 `Overwrite` 交给上游 reducer channel：
   `TypeError: can only concatenate list (not "Overwrite") to list`
   （`langgraph/channels/binop.py` 对**它自己的** `Overwrite` 做 isinstance）。
2. `ToolNode` 对 `isinstance(output, Command)` 做判定
   （`langgraph/prebuilt/tool_node.py:881/898/1474/1492`）：
   工具返回原生 `Command` 会被**静默忽略**。

因此迁移顺序被修正为：

> **类型与其消费者必须在同一个切片内一起替换；不能先批量改 import。**

截至本次更新，agents/middleware、`create_agent`、checkpointer、工具路径、
goal continuation 和 `custom` stream mode 已进入同切片迁移；sqlite/postgres
与 redis cache 已原生实现。尚未覆盖的生产语义仍必须 fail closed（抛错），
禁止静默降级。

## 8. 当前能力边界

当前证据足以说明 ReactiveGraph 已能作为真实 DeerFlow fork 的底层运行关键路径，
并保持 replay、checkpoint/recovery、delta rollback/resume 的上游语义一致。它
**还不足以**说明 DeerFlow 已全面替换，也不足以说明已经在功能、稳定性或性能上
全面超过 LangGraph/LangChain。完成全面比较至少还需要：

1. 所有 durable provider 的逐条行为等价（sqlite/postgres/redis 已原生实现并
   有真实服务证据，delta cache 路径仍需补齐）；
2. 完整 rollback/fork、跨进程 ownership 和 gateway/channels 生产语义；
3. sandbox/MCP/skills/frontend 的同输入行为对照；
4. 相同负载下与 upstream 的双跑正确性、延迟、吞吐和恢复性基准。
