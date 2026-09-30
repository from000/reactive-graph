# ReactiveChain

声明式管道组件层，构建在 ReactiveGraph 反应式引擎之上。

```python
from reactivechain import RunnableLambda

chain = RunnableLambda(lambda s: {"x": int(s["n"]) + 1}) | RunnableLambda(
    lambda s: {"y": s["x"] * 2}, reads={"x"}
)
out = chain.invoke({"n": 1})  # {"y": 4}
```

- **管道式 `|` 组合**：与 LangChain LCEL 相同的心智模型，便于迁移。
- **段级选择性执行**：每段声明 `reads`/`writes`，输入指纹不变即跳过段调用
  （进程内缓存；图集成时包装为 ReactiveGraph task）。
- **图集成**：`.to_graph()` 把管道包装为 ReactiveGraph 图，复用状态/持久化/
  流式/因果 trace。

## 组件索引（对标 LangChain）

| 面 | 组件 |
|---|---|
| 核心 | `Runnable`、`RunnableLambda`、`RunnablePassthrough`、`GeneratorRunnable`、`Pipeline`（`\|`） |
| 组合 | `RunnableParallel`、`RunnableBranch`、`RunnableFallback`、`RunnableAssign`、`with_retry`/`with_fallbacks`/`map` |
| 提示/消息 | `PromptTemplate`、`ChatPromptTemplate`、`MessagePlaceholder`、`FewShotPromptTemplate`、`PipelinePromptTemplate`、`BaseMessage` 家族 |
| 模型 | `BaseLanguageModel`、`OpenAICompatChatModel`（纯 stdlib）、`AnthropicCompatChatModel`、`FakeLLM`、`bind_tools` |
| 解析 | `StrOutputParser`、`JsonOutputParser`、`JsonRegexParser`、`CommaSeparatedListOutputParser`、`PydanticOutputParser`、`EnumOutputParser`、`DatetimeOutputParser`、`OutputFixingParser`、`RetryOutputParser`、`RetryWithErrorOutputParser` |
| 记忆 | `ConversationBufferMemory`、`ConversationBufferWindowMemory`、`ConversationSummaryMemory`、`ConversationTokenBufferMemory`、`SummaryBufferMemory`、`EntityMemory`（thread 维度 `MemoryStore`） |
| 文档/检索 | `Document`、`TextLoader`/`CSVLoader`/`JSONLoader`/`UrlLoader`/`DirectoryLoader`、`TextSplitter` 家族、`Embeddings`、`HashEmbeddings`、`InMemoryVectorStore`/`DriverVectorStore`、`VectorStoreRetriever`/`BM25Retriever`/`MultiQueryRetriever`/`EnsembleRetriever` |
| 工具/Agent | `@tool`、`StructuredTool`、`Toolkit`、`ToolNodeAdapter`、`from_openapi`（OpenAPI 服务→工具）、内置工具（calculator/time/date/web_search 占位；`terminal` 危险、需显式 `from reactivechain.tools import terminal`）、`create_tool_calling_agent`、`create_react_agent`、`AgentExecutor` |
| 可观测 | `BaseCallbackHandler`、`CallbackManager`、`ChainStats`（耗时/跳过段数/token） |

## 教程

- 01 管道入门：`docs/tutorials/reactivechain-01-pipeline.md`
- 02 RAG 检索增强生成：`docs/tutorials/reactivechain-02-rag.md`
- 03 工具型 Agent：`docs/tutorials/reactivechain-03-tool-agent.md`

测试：`uv run --directory python/reactivechain --extra test pytest`；
差分基准：`uv run --directory benchmarks/differential python run_reactchain_differential.py`。

设计文档：`docs/plans/2026-09-15-reactivechain-design.md`。

## Tools

ReactiveChain tools use the industry-standard OpenAI function-calling schema and add runtime metadata:

- `ToolResult(content, artifact, patches, return_direct)` separates model content, program artifacts, and state patches.
- `InjectedState` / `InjectedToolCallId` are runtime-injected and omitted from the model-facing schema.
- `InjectedSecret` and `InjectedSecretArg("ENV_KEY")` inject secrets by environment key without exposing them in model schemas.
- `@tool(reads=..., writes=..., kind=..., receipt=..., return_direct=..., on_error=...)` declares state/effect semantics and propagates them into `ToolSpec`.
- `_arun` provides async execution.
- `terminal` is disabled by default; enable it with `TerminalPolicy` and an explicit command allowlist.
