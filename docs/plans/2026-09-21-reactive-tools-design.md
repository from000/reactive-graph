# Reactive Tools Design — 行业标准协议 + Reactive 契约

日期：2026-09-21 · 状态：已批准（路线 B：Native Tool 主路径 + 可选生态 Adapter）

## 目标

将 `reactivechain.tools` 从“函数包装器”升级为可调度、可审计、可幂等的工具运行时协议：

1. 兼容行业通用工具协议：JSON Schema、OpenAI tool calling、tool call id、async 工具、artifact。
2. 引入 Reactive 契约：`reads` / `writes` / `kind` / `receipt` / `return_direct` / injected state。
3. 保持核心零强依赖：不复制 LangChain 的 `BaseTool` / callback / Serializable 体系。
4. 后续可选提供 `from_langchain_tool` adapter，但不进入核心依赖。

## 非目标

- 不复制 LangChain 的 48 个 partner 集成或大量内置工具。
- 不在本阶段实现 MCP wire 协议、流式工具输出、完整 middleware 图。
- 不把 LangChain 作为运行时依赖。

## 核心数据结构

```python
@dataclass
class ToolResult:
    content: Any
    artifact: Any | None = None
    patches: list[dict[str, Any]] = field(default_factory=list)
    return_direct: bool = False
```

- `content`：给模型看的文本/结构化结果。
- `artifact`：给程序使用，不进入模型上下文。
- `patches`：工具返回给 ReactiveGraph 的状态更新。
- `return_direct`：工具结果应终止 agent 循环。

## 工具契约

```python
@tool(
    reads={"user.id"},
    writes={"search.results"},
    kind="pure",
    return_direct=False,
)
def search(query: str, state: InjectedState) -> ToolResult: ...
```

- 默认 `kind="effect"`：保守处理，不因未知副作用误跳过。
- 默认 `reads/writes=None`：未知依赖，作为 opaque/effect 执行。
- `InjectedState` / `InjectedToolCallId` 参数不进入 tool schema，由 runtime 注入。
- Pydantic schema 后续通过可选 extra 或手写 schema 支持，不引入核心依赖。

## 错误策略

```python
@tool(on_error="return")
```

- `return`：默认，错误字符串回传模型。
- `raise`：直接抛出，适合生产显式失败。

## 内置工具安全

- `terminal` 保持存在但默认不可用，需显式启用 `TerminalPolicy`。
- `web_search` 保留 adapter 占位，不冒充真实搜索。

## 验收

- schema：Injected 参数不暴露给模型。
- execution：artifact 与 content 正确分离。
- state：ToolResult.patches 被合并到工具输出。
- agent：return_direct 终止循环。
- async：`_arun` 默认包装 `_run`。
- safety：terminal 默认拒绝执行。
