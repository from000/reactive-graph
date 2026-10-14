# ReactiveChain ↔ LangChain 逐组件源码级对照（2026-09-15）

> 目的：如实回答"对标 langchain 源码完成度"。本文档逐组件对照
> ReactiveChain（`python/reactivechain/`）与上游 **langchain_core 1.6.2**
> 的对应实现（源码实测于
> `benchmarks/differential/.venv/.../site-packages/langchain_core`）。
> 分级：Critical=行为/签名不兼容 · Important=语义差异可感知 · Minor=命名或默认值差异。
>
> **环境限制（如实）**：差分 venv 仅安装 `langchain_core 1.6.2`（+ langgraph
> 1.2.11 / prebuilt / langsmith），**无 `langchain`、`langchain_community`
> 包**——`ConversationBufferMemory`、OpenAI 嵌入实现、`langchain.agents.
> AgentExecutor` 的 langchain 侧无法源码核对，标注"上游无对应"或据已知
> API 语义判定。

## 总论

- **统一状态契约是核心设计差异**：ReactiveChain 每段 `invoke(State)→写键 dict`，
  按 `reads/writes` 提取/写入顶层状态，配段级指纹缓存（同输入跳过段）——
  与 langchain 的"值直通管道"（上一步输出原样为下一步输入）**有意不兼容**。
  这是构建在 ReactiveGraph 状态引擎上的既定设计（设计文档 §3），不是实现缺陷。
- **直接可移植面小**：除少数 Minor 项（`with_retry` 参数名、Document 字段、
  Embeddings 签名、tool JSON schema 的 type/required 映射），大部分组件存在
  至少一项 Critical——均为 API 形状差异（dict 状态 vs 值直通 / BaseMessage 对象
  vs API dict），不是功能缺失。
- 前文已有两层实证：LCEL 4 场景差分输出一致（`docs/benchmarks.md`）、
  langgraph StateGraph 语义对照一致。本表补**实现级**差异清单。

## 1. 核心 runnable（runnable.py ↔ langchain_core/runnables/base.py）

| 项 | ReactiveChain | LangChain | 级别 |
|---|---|---|---|
| invoke 形状 | 完整状态 dict → 段按 reads 提取 → 返回写键 dict；Pipeline 返回末段输出 | 输入原样逐段直通，返回最后一步原始值 | Critical |
| stream | 覆写 stream 的段逐块 yield `{"chunk": ...}`（中间块） | 全链 transform，chunk 为最终输出类型（含合并 token） | Critical |
| 选择性缓存 | `(段下标, 段id)→输入指纹` 段级跳过（batch 批内共享、clear_cache） | 默认无（仅模型级 cache 配置） | 独有语义 |
| batch | 串行逐项 | 线程池并发 + `return_exceptions` | Important |
| 方法集 | invoke/stream/batch/pipe/\| | 完整 async（ainvoke/astream/…）+ config + get_graph + 序列化 | Important |

## 2. 提示/消息（prompt.py · messages.py ↔ langchain_core/prompts/ · messages/）

| 项 | ReactiveChain | LangChain | 级别 |
|---|---|---|---|
| PromptTemplate 输出 | `{"prompt": str}` | `StringPromptValue` 对象 | Critical |
| 缺失变量错误 | `ReactiveChainError`（RuntimeError） | `KeyError` | Important |
| ChatPromptTemplate | `{"messages": [API dict]}`（human→user 角色改写） | `ChatPromptValue.messages` 为 **BaseMessage 对象列表**（保留 system/human/ai） | Critical |
| 消息项 | `(role, template)` / MessagePlaceholder | BaseMessage / BaseMessagePromptTemplate / placeholder 简写 / optional | Important |
| BaseMessage 字段 | content/role/name/additional_kwargs + `to_api_dict()`（OpenAI dict） | content 可多模态、type/id/response_metadata、`model_dump()` LC 格式 | Important（序列化不互通） |
| FewShot | 手写 `%%EXAMPLES%%` 替换 | suffix/prefix/example_prompt/example_separator 显式参数 | Important |

## 3. 模型层（llm.py ↔ language_models/chat_models.py）

| 项 | ReactiveChain | LangChain | 级别 |
|---|---|---|---|
| invoke 返回 | `{"output": str, "llm_message": dict}` | **AIMessage 对象**（content/tool_calls/usage_metadata） | Critical |
| stream 块 | `{"chunk": str}`（SSE 自解析） | `AIMessageChunk`（自动合并 + 末尾补空块） | Critical |
| bind_tools | 原地改写 `self._tools` 返回 self | 返回**新 Runnable**（绑定副本）+ tool_choice | Critical |
| 构造 | OpenAICompat(base_url, api_key, model, ...) | provider 子类 + pydantic 字段模型 | Important（模块化方式不同） |

## 4. 解析器（parsers.py ↔ output_parsers/）

| 项 | ReactiveChain | LangChain | 级别 |
|---|---|---|---|
| StrOutputParser | 读状态 `output` 键 → `{"output": str}` | 输入 str\|BaseMessage → 裸 str | Critical |
| 错误类型 | `ReactiveChainError`（RuntimeError） | `OutputParserException`（ValueError 子类，带 llm_output） | Critical |
| JsonOutputParser | 手工括号扫描取首个 JSON（更宽容） | parse_json_markdown（支持 ```json 块）+ partial 语义 | Important |
| PydanticOutputParser | dataclass 或 pydantic，额外写 `format_instructions` 键 | 仅 pydantic 模型，invoke 返回模型实例 | Important |
| Enum/Datetime | 本包内 | 在 `langchain` 包（未装，仅据序列化映射推断） | Important |

## 5. 组合/回调（combiner.py · callback.py ↔ runnables/ · callbacks/）

| 项 | ReactiveChain | LangChain | 级别 |
|---|---|---|---|
| RunnableParallel | 顺序循环（注释自认"语义并行"） | 真并发（线程池/async gather） | Important |
| RunnableBranch | `(branches=[(pred, r)...], default)` | 变参扁平 `(c0, r0, c1, r1, ..., default)` | Critical |
| RunnableAssign | 分支 dict **扁平合并** | 结果**嵌套**在分支名键下 | Critical |
| handler 签名 | `on_chain_start(runnable, state)` 等自定事件 | `on_chain_start(serialized, inputs, *, run_id, ...)` 官方协议 | Critical |
| 分发 | emit 直接调用 + 全局单例 | run manager 树 + configure 继承 + 前缀化 | Critical |
| ChainStats/instrument_pipeline | 独有（选择性可观测） | 对应 langsmith tracers | 导出面 |

## 6. 记忆/检索/工具/Agent（扩展面）

| 项 | ReactiveChain | LangChain | 级别 |
|---|---|---|---|
| Memory | `load()→{"history": list[BaseMessage]}`、`save(inputs, outputs)`、thread_id 维度 + 原子 update | `load_memory_variables`/`save_context`（langchain 包未装，键形状据已知语义对齐） | Important（签名）/Critical（无对应类可核对） |
| Retriever.invoke | `invoke(State)→{"docs": [...]}` | `invoke(str)→list[Document]`（文档形状同构） | Critical（签名） |
| VectorStore | add_documents/similarity_search/delete（子集） | + add_texts/search 三型/mmr/get_by_ids/async 全家 | Important |
| @tool schema | properties/required/type 映射一致；**Literal/Enum 兜底 string、嵌套 dict 不展开** | enum 列表 + $defs 展开 | Important |
| tool_calling 终止 | `not tool_calls → return`；超 max_iterations raise；工具输出加"不可信"前缀 + 截断；不校验 tool_call id | langgraph should_continue 等价终止；recursion_limit 异常；ToolNode 无前缀；校验每个 tool_call id 有对应 ToolMessage | Minor（终止）/Important（包装与校验） |

## 结论

1. **组件层功能面（M1-M6）全部有实现、有测试（155 passed）**；与 langchain
   的同名/对应 API 的**行为差异绝大多数来自统一状态 dict 契约**——这是
   ReactiveGraph 生态的既定设计，LCEL 差分与 StateGraph 语义对照已验证
   "同语义链输出一致"。
2. **若需跨框架移植**，聚焦点依次为：invoke 入参协议（Retriever/BaseTool/
   LLM/StrOutputParser）、消息对象 vs dict 序列化、bind_tools 无副作用化、
   handler 事件签名。
3. **未核对项（环境限制）**：`langchain`/`langchain_community` 包内的
   memory/embeddings/AgentExecutor 实现（对应 venv 未安装，仅据已知 API
   语义标注）；如需可另装包补核。

## 已强化（2026-09-15，docs/plans/2026-09-15-reactivechain-strengthening.md）

按"核心自研、行业规范直接实现"方向推进后的状态变化：

| 项 | 之前 | 现在 |
|---|---|---|
| 图集成 | `to_graph()` 整链单 effect task | **段级多 task 图**：每段一个 task（`seg_i`）、writes 声明保留、`pure_segments` 参数标记引擎选择性段；in-process fallback 链式累积（`python/reactivegraph` fallback 语义同步升级） |
| async 面 | 无 | `Runnable.ainvoke/astream/abatch`（`asyncio.to_thread` 包装 + Pipeline 缓存线程锁） |
| provider | OpenAI 兼容 | + **Anthropic Messages API**（`AnthropicCompatChatModel`：`/v1/messages`、`x-api-key`+`anthropic-version`、system 拆分、SSE 流式、4xx 不重试） |
| @tool schema | Literal/Enum 兜底 string、嵌套 dict 不展开 | **JSON Schema 规范对齐**：Literal/Enum → `enum` 列表、`dict[str,X]` → `additionalProperties`、TypedDict → 递归 `properties`+`required` |

验证：reactivechain 164 passed（新增 7 测试）、reactivegraph 92 passed、
覆盖率 89%、ruff 全绿（含 tests 目录存量清理）。

行业规范结论（用户确认）：langchain 的 tool/消息格式/SSE 等多为行业规范
（OpenAI/Anthropic/JSON Schema 定义）的具体实现——ReactiveChain 按规范直接
实现即可，无需 copy 其代码；核心差异化（段级选择性、状态引擎、可观测）自研。
