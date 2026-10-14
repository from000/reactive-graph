# ReactiveChain 设计文档 — 完整组件层（对标 LangChain 全功能面）

> 日期：2026-09-15 ｜ 状态：设计草案，待评审
> 定位：ReactiveGraph 生态的应用组件层，对标 `langchain` 的功能面，但执行内核是
> **ReactiveGraph 反应式图**——声明式管道编译成图，运行期只执行受影响段。
> 哲学与 ReactiveGraph 一致：**能力对齐 LangChain、形状按反应式语义重设计**，
> 不镜像 LCEL 的内部实现，不引入对 langchain 的运行时依赖。

## 1. 决策记录（已澄清）

| 决策点 | 结论 |
|---|---|
| 语言与运行位置 | **Python 本地优先**（建在 `python/reactivegraph` 之上，进程内编译成图） |
| 组合语法 | **管道式 `\|`**（`prompt \| llm \| parser`），LCEL 心智模型、利于迁移 |
| 组件范围 | **全功能面**：核心可运行对象 + 提示 + 模型 + 解析 + 文档/检索 + 记忆 + 工具 + Agent + 组合器 + 可观测 |
| 成功标准 | 端到端教程（RAG + 工具链）+ 差分基准（LangChain vs ReactiveChain 同管道对比） |
| 包形态 | 新包 `python/reactivechain`（独立 uv member，依赖 `reactivegraph>=0.1.0`） |

## 2. 架构总览

```
┌─────────────────────────────────────────────────────────┐
│ reactivechain（本设计）                                  │
│  Runnable 抽象 · 管道编译器 · 组件库 · 回调/追踪          │
│  编译期：│ 表达式 → 依赖图（task + 数据流边）             │
│  运行期：ReactiveGraph Driver 选择性执行（指纹/缓存/事件） │
├─────────────────────────────────────────────────────────┤
│ reactivegraph（已有）：GraphBuilder / Scheduler /         │
│   State / Transaction / prebuilt(ToolSpec/ToolNode/      │
│   ReactiveAgent/ReactiveGraphClient) / otel / checkpoint │
└─────────────────────────────────────────────────────────┘
```

**核心概念（与 LCEL 的对应）**：

| 概念 | LangChain | ReactiveChain | 差异点 |
|---|---|---|---|
| 可运行对象 | `Runnable`（invoke/stream/batch） | `Runnable` 基类（invoke/stream/batch） | 相同 |
| 管道 | `a \| b` → `RunnableSequence` | `a \| b` → `Pipeline` | 相同语法；**编译期产出图** |
| 依赖声明 | 隐式（按输入 dict 键自然流动） | **显式 read/write 集**（`declares_reads/writes`）由编译器静态分析或组件声明 | 反应式核心：读集不变即跳过 |
| 执行 | 顺序调用 | 编译为 `ReactiveGraph` 图，事件路由 + 指纹跳过 + computed 缓存 | **选择性执行** |
| 流式 | 生成器透传 | 生成器透传，经 RGP/1 也可远程 | 相同 |

**管道编译规则（§3）**：`A | B | C` 中每段是一个 pure/effect task；
段间依赖 = B.reads ∩ A.writes（或显式 `on=` 事件）；同段输入指纹不变
（如 retriever 查询未变）→ 整段跳过。**这是 ReactiveChain 的差异化卖点**：
LangChain 中任何一步输入未变也会全链重跑，ReactiveChain 只跑受影响的段。

## 3. 管道编译器（PipelineCompiler）

- 输入：任意 `Runnable` 组合树（`\|`、`RunnableParallel`、`RunnableBranch`、`RunnableFallback` 等）
- 输出：`ReactiveGraph`（可 `.invoke()` / `.stream()`），模块可单独复用
- 规则：
  1. 叶子 Runnable 声明 `reads: set[str]` / `writes: set[str]`（组件自带默认；用户可覆写）
  2. 顺序管道：后段 reads ⊆ 前段 writes ∪ 输入 → 前段写路径失效后段（computed 语义）
  3. 分支/并行：各分支独立子图，事件 `run` 同时触发（并行批）
  4. fallback：主段失败事件 → 备选段（effect task + retry 策略）
  5. 流式段（生成器 handler）→ streaming task（沿用 driver stream/mux）
- 错误处理：段异常 → 图级错误事件 → 可选 retry/fallback；`RecursionLimitError` 语义沿用
- 校验：编译期检查 read 未声明（静态分析 handler 签名/注解，或要求显式声明——v1 显式优先，分析器兜底）

## 4. 组件清单（LangChain 功能面对照）

### 4.1 核心 Runnable 与组合算子
| 组件 | 说明 |
|---|---|
| `Runnable`（基类） | `invoke/stream/batch/ainvoke/…` 同步+异步；`\|`、`bind`、`with_fallbacks`、`with_retry`、`map` |
| `RunnableLambda`/`RunnableGenerator` | 任意函数/生成器包装（自动 read/write 推断失败时报错并提示显式声明） |
| `RunnablePassthrough`/`RunnableAssign` | 透传 / 注入字段 |
| `RunnableParallel` | 并行分支（编译为并行批 task） |
| `RunnableBranch` | 条件路由（谓词函数 → 分支） |
| `RunnableFallback` | 异常回退（fallback 链） |
| `RunnableSequence`（`\|` 产物） | 顺序管道 |

### 4.2 消息与提示（对应 langchain_core.messages / prompts）
| 组件 | 说明 |
|---|---|
| `BaseMessage` 家族 | Human/AI/System/Tool/Function/GenericMessage；内容、tool_calls、附加元数据 |
| `PromptTemplate` | `{var}` 插值 + `partial` 部分变量 + `.format/.format_prompt` |
| `ChatPromptTemplate` | 消息列表模板 + `MessagePlaceholder`（工具结果/历史插槽） |
| `FewShotPromptTemplate` | 示例集 + 动态选择器（长度/语义选择器接口） |
| `PipelinePromptTemplate` | 多模板组合 |
| Prompt 序列化 | `dumps/loads`（JSON）与安全校验（禁止任意代码执行） |

### 4.3 模型层（对应 langchain.llms/chat_models）
| 组件 | 说明 |
|---|---|
| `BaseLanguageModel` | invoke/stream/generate 抽象（含 tool calling 回调） |
| `OpenAICompatChatModel` | OpenAI 兼容 `/chat/completions`（**纯 stdlib urllib 实现**，复用 tutorial 08：SSL ctx + 重试 + SSE 流），支持 tools、stop、temperature、max_tokens、json_mode |
| `FakeLLM`/确定性模型 | 测试与教程用 |
| `bind_tools` | 工具模式切换（返回 tool_calls 结构化消息） |
| 多 provider | 通过 base_url + api_key 参数天然支持（v1 只落 OpenAI 兼容，接口留扩展位） |

### 4.4 输出解析（对应 langchain_core.output_parsers）
| 组件 | 说明 |
|---|---|
| `StrOutputParser` | 文本直通（消息 → 内容） |
| `JsonOutputParser`/`JsonRegexParser` | 容错 JSON 提取 |
| `PydanticOutputParser`/结构化输出 | 基于类型注解（dataclass/pydantic 可选依赖）生成 schema → `json_schema` 提示注入 + 校验 |
| `CommaSeparatedListOutputParser` / `EnumOutputParser` / `DatetimeOutputParser` | 小工具解析器 |
| `OutputFixingParser` | 解析失败回调 LLM 修复（包装器） |
| `RetryOutputParser`/`RetryWithErrorOutputParser` | 重试语义（配合 with_retry） |

### 4.5 文档与检索（对应 langchain_core.documents / langchain.text_splitter / retrievers）
| 组件 | 说明 |
|---|---|
| `Document` | page_content + metadata + id |
| `BaseLoader`（text/csv/json/url/目录） | **纯 stdlib 实现**（v1：text/csv/json/url；html/pdf 等留扩展位或可选依赖） |
| `TextSplitter`（Recursive/Character/Token/按分隔符） | 纯 Python 实现；TokenSplitter 用近似计数，tiktoken 作可选依赖 |
| `Embeddings` 接口 | `embed_documents/embed_query` 抽象；OpenAI 兼容实现（stdlib urllib）；hash 嵌入（测试） |
| `VectorStore` | 内存实现 + **接入既有 `storage/vector.ts` VECTOR_* 协议**（经 Driver host，可持久化） |
| `Retriever`（VectorStoreRetriever/BM25/MultiQuery/Ensemble/自建） | 检索返回 Document 列表；`invoke` 输入 query → 输出 docs |
| RAG 链 | `retriever \| prompt \| llm \| parser` 官方模板（教程载体） |

### 4.6 记忆（对应 langchain.memory）
| 组件 | 说明 |
|---|---|
| `BaseMemory` / `BaseChatMemory` | 加载/保存会话历史接口 |
| `ConversationBufferMemory` / `ConversationBufferWindowMemory` | 全量 / 滑动窗口 |
| `ConversationSummaryMemory` / `SummaryBufferMemory` | LLM 摘要（复用 LLM 组件） |
| `ConversationTokenBufferMemory` | token 截断 |
| `EntityMemory` | 实体抽取+存储 |
| 持久化 | 存 `thread_id` 维度，挂到现有 checkpoint/thread 语义（RGP/1 STORE_OP） |

### 4.7 工具（对应 langchain.tools）
| 组件 | 说明 |
|---|---|
| `BaseTool` / `@tool` 装饰器 | 函数 → 工具（args schema 从注解推导，docstring 作描述） |
| `StructuredTool` | 显式 args schema |
| `Toolkit` | 工具集组合 |
| `ToolNode` 对接 | 复用 `reactivegraph.prebuilt.ToolNode`（driver 侧执行） |
| 内置工具 | 纯实现：Calculator、terminal、时间/日期、搜索占位（HTTP 包装）；需第三方服务的留 adapter 接口 |

### 4.8 Agent（对应 langchain.agents）
| 组件 | 说明 |
|---|---|
| `create_react_agent`（reactivechain 版） | 复用 prebuilt `ReactiveAgent` 内核，接收管道式 prompt+llm |
| `create_tool_calling_agent` | 基于 `bind_tools` 的原生 tool calling 循环 |
| `AgentExecutor` 等价 | 编译为图：agent 节点 ↔ tool 节点回环（事件循环 + 递归上限） |
| 多 agent | 后续（图内多 agent 节点天然支持，列为后续） |

### 4.9 可观测与回调（对应 langchain callback / tracing）
| 组件 | 说明 |
|---|---|
| `BaseCallbackHandler`（on_llm_start/end、on_chain_start/end、on_tool_start/end、on_retry、on_stream） | 同步+异步 |
| `CallbackManager` | 全局/链级 handler 注入 |
| Trace 导出 | 管道执行轨迹 → 复用 driver CausalTrace/otel SpanEmitter（task span、retry、computed hit） |
| `stats`（token 用量、耗时、跳过段数） | 选择性执行的可观测证据（成功标准之基准的输入端） |

### 4.10 应用模板（后续，不在 M1–M6 内）
RAG、SQL 问数、摘要、多文档问答、multi-agent 编排——以教程+配方形式沉淀，不新造运行时概念。

## 5. 里程碑（M1–M6，各带独立 commit + failing-then-passing 测试）

### M1 核心运行时可与管道编译（Runnable 基类 + `\|` + PipelineCompiler + invoke/stream/batch）
- 验收：`A | B | C` 编译为 ReactiveGraph 图；纯函数段、生成器段（流式）跑通；
  `.invoke/.stream/.batch` 语义正确；显式 read/write 声明 + 静态分析兜底；
  未声明且不可推断时报错（提示信息同项目 Hint 风格）
- 测试：pipeline 单元测试（顺序/并行/嵌套）；与手写 GraphBuilder 的等价性断言

### M2 提示与消息 + 模型层（4.2 + 4.3）
- 验收：ChatPromptTemplate 全家族；OpenAICompatChatModel 端到端（流式 + tool calling + json_mode）；
  FakeLLM 测试回路；`bind_tools`；消息→提示→LLM→解析的完整管道 tutorial 雏形
- 测试：模板渲染、消息合并、LLM 客户端 mock 化（不依赖真实网关）

### M3 输出解析 + 记忆（4.4 + 4.6）
- 验收：parser 家族含 OutputFixing/Retry；memory 家族含持久化（thread 维度）
- 测试：解析容错矩阵、记忆窗口/摘要边界

### M4 文档与检索（4.5）
- 验收：Loader/Splitter/Embeddings/VectorStore（内存 + 既有 vector 协议）/Retriever 全家；
  检索段的选择性执行基准数据（同 query 重跑时 retriever 段跳过）
- 测试：splitter 边界、检索一致性；RAG 链端到端

### M5 工具与 Agent（4.7 + 4.8）
- 验收：`@tool`/Toolkit；tool-calling agent 与 ReAct agent 编译为图回环；递归上限/中断
- 测试：tool schema 推导、循环终止性、中断恢复

### M6 组合器 + 可观测 + 验证收口（4.1 补全 + 4.9 + 成功标准）
- 验收：
  - RunnableParallel/Branch/Fallback/Passthrough 全通
  - 回调 + trace 导出 + stats（token/耗时/跳过段数）
  - **差分基准**：同语义管道（RAG + 工具链）在 `langchain` vs `reactivechain` 同机对比，
    采用项目差分传统（输出断言一致 + median×3 + no-gain 诚实报告）
  - **端到端教程**：`docs/tutorials/` 新增 3 篇（01 管道入门 / 02 RAG / 03 工具型 agent），
    CI 校验（scripts/check-tutorials.py 接入）
- 测试：组合器矩阵、回调顺序、基准脚本入库（benchmarks/ + .results 记录机环境）

## 6. 质量与纪律

- 每功能独立 commit + failing-then-passing 测试（沿用项目执行纪律）
- 覆盖率门槛：与仓库一致（pytest cov ≥80%，核心编译路径 ≥85% 为目标）
- 依赖纪律：**运行时零第三方依赖**（纯 stdlib）；pydantic/tiktoken 等仅作可选 extra
- 文档纪律：每个组件一节 API 名+示例；README 给三行入门
- CI：`ci.yml` 增加 reactivechain python job（3.10–3.12）+ docs 教程检查接入

## 7. 风险与开放问题

| 风险/问题 | 应对 |
|---|---|
| "语言模型功能 vs 图执行"的语义张力：LLM 调用是 effect（不可跳过），检索是 pure（可跳过） | 组件类型显式化：`pure`（可指纹跳过）/ `effect`（每次执行）；默认 LLM=effect、retriever=pure，用户可覆写 |
| LangChain 隐式数据流 vs 显式 read/write 声明的心智负担 | 静态分析（读 handler 签名/注解）+ `declares_*` 覆写；模板对 LangChain 用户近乎透明 |
| 范围过大（全功能面）导致面面俱到、深度不足 | M1–M6 顺序交付，每里程碑独立可用；教程与基准先行验证内核价值 |
| 与既有 prebuilt（ToolNode/ReactiveAgent）重叠 | 复用不重写：prebuilt 是 driver 侧执行体，reactivechain 是声明层与编译层 |
| OpenAI 兼容 vs 其他 provider | 抽象 `BaseLanguageModel` + base_url 注入；真实多 provider 适配为后续 |

## 8. 后续（非 M1–M6）
- TS 版 reactivechain（同构 API，封装 sdk-js）
- 应用模板库（§4.10）
- 远程执行：管道编译产物经 gateway/SDK 部署（对标 LangGraph Platform）
- 生态：与 ReactiveGraph 主仓库发布节奏同步（reactivechain 首个版本随 0.2.0）

## 9. 与发布的关系
ReactiveChain 不阻塞 ReactiveGraph 0.1.0 发布（主包先发）；reactivechain 作为独立
uv member 加入 workspace，目标随 0.2.0 或独立 0.1.0 发布。当前阻塞项仍为
npm/PyPI secrets 配置与 `v0.1.0` tag 推送。