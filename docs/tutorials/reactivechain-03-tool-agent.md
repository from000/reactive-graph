# ReactiveChain 03 · 工具型 Agent

**目标**：`create_tool_calling_agent` 跑通 模型→tool_calls→工具→模型 循环，验证递归上限保护。

## 核心（M5）

- `@tool` 装饰器：docstring 作描述、类型注解推导 args schema；`Toolkit` 组合去重
- `create_tool_calling_agent(llm, toolkit, system_prompt=..., max_iterations=...)`：
  基于 `bind_tools` 的原生 tool calling 循环；工具异常捕获为 `Error: ...` 消息回传 agent
- `create_react_agent`：复用 `reactivegraph.prebuilt.ReactiveAgent` 内核（思考→工具→观察）
- 递归上限：超过 `max_iterations` 抛 `ReactiveChainError`（Hint 提示）
- 真模型：`OpenAICompatChatModel(base_url, api_key, model)`（纯 stdlib，SSE 流式）

## 运行

```bash
uv run --directory python/reactivechain python docs/tutorials/code/reactchain_03_tool_agent.py
# 3) agent 回答： 结果是 14
# 4) 上限保护： agent 超过 max_iterations=3 仍未作答 — Hint: ...
```

## 进阶

- 内置工具：`calculator`（安全 AST 求值拒绝注入）、`current_time` / `current_date`、`web_search`（占位 adapter）；`terminal`（shell 直通，**危险**）需显式 `from reactivechain.tools import terminal` 后自行评估
- 图集成：`AgentExecutor(agent).to_graph(graph_id=...)` 包装为 ReactiveGraph 图
- 工具 schema 与 `bind_tools` 同构：`ToolNodeAdapter(toolkit).node()` 对接 prebuilt `ToolNode`