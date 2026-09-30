# 全功能 e2e 验证套件

对仓库**当前公开支持的功能面**逐一端到端验证（43 功能点，见 `FEATURE_MATRIX.md`
——唯一事实源，每功能一行：verified / unverified(原因) / pending）。

设计：`docs/plans/2026-09-17-e2e-full-coverage-design.md`
计划：`docs/plans/2026-09-17-e2e-full-coverage-implementation.md`

## 运行

```bash
# 全部 e2e
uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/ -q

# 单域
uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/test_e2e_engine.py -q
uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/test_e2e_driver.py -q
uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/test_e2e_chain.py -q
uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/test_e2e_scenarios.py -q
```

## e2e 定义

- 引擎/D 域：完整真实路径——Python API → DriverHost **真 Driver 子进程**（sqlite 持久化）→ 断言。
- 链纯 Python 组件：组件调用链即完整路径（parsers/prompt/memory 等无 Driver 参与）。
- 外部依赖（OpenAI 真实 API / Postgres / Redis / TCP/TLS 网关）：环境受限 → 矩阵标
  `unverified`，不做 mock 冒充。

## 结构

- `conftest.py` — sys.path 锚点 + `e2e_host`（DriverHost + 临时 sqlite）/ `e2e_host_memory` fixture
- `test_e2e_engine.py` — 域 A（A1–A14）
- `test_e2e_driver.py` — 域 B（B1–B7）
- `test_e2e_chain.py` — 域 C（C1–C16）
- `test_e2e_scenarios.py` — 域 D（D1–D6）

## 验证记录（2026-09-17）

**43/43 全绿**（A 14 + B 7 + C 16 + D 6），`43 passed`。矩阵终态见
`FEATURE_MATRIX.md`。

**e2e 暴露的框架差异/缺陷（跟进状态）**：

1. ~~host/Driver 跨 run 无缓存~~ **已修复**（`d0183d4`）：`SchedulerPersistent`
   持久化 pure 任务指纹到编译图，Driver 每次 run 新建 `Scheduler` 后同输入
   跨 run 跳过仍生效；指纹改基于 run 输入（baseInput，与 effect 幂等键一致，
   避免 store 累积致指纹恒变）。A2 已有 host 跨 run 跳过断言。
2. **host 不支持 computed（设计边界）**：computed 声明在 Python 侧 GraphDef，
   但编译载荷不含 `computeds`（`graph.py` 的 wire 面），Driver `GraphSpec` 亦无
   该字段——根因是 computed 的 `selector` 是 Python 函数，不可跨语言序列化给
   TS Driver 执行。fallback 内核支持（`test_native_graph.py`
   `test_fallback_computed_evaluated`）。
3. **`CHECKPOINT_OP put` 的 record.values** 需 Driver 内部序列化 Buffer 格式
   （JsonPlusSerializer/EncryptedSerializer）——JSON 协议传输后 Python 面
   不可直接构造，属 TS 内部细节（get/list/delete_thread 已验证）。

**unverified（环境受限，非功能缺失）**：OpenAICompatEmbeddings 真实端点（C4——
AGNES 网关 embeddings 支持未单独验证）、Postgres/Redis 持久化后端（B5）、
web_search 外网面（C12）。LLM 真实调用（C3）已通过 AGNES 网关验证（invoke + stream）。
