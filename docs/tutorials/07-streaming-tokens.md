# 07 · LLM token 级流(生成器回调)

**目标**:任务函数写成生成器,`stream()` 就实时看到 token——远程打字机效果,
无需任何额外布线。

## 前置

```bash
pnpm --filter @reactivegraph/driver build   # 需要 node ≥ 20
```

## 生成器回调 = token 流

普通任务函数返回 `dict`,一次提交。若函数是**生成器**,每个 `yield` 会成为一条
transient `messages` 事件透出到 `stream()`;生成器的 `return` 值才是提交给
Driver 的结果(必须是完整 `TaskSuccess` 形状:patches/writes/return_value/
external_receipts)。

```python
def llm(state):
    for token in model_tokens:
        yield token                      # → messages 事件(实时可见)
    return {"patches": [...], "writes": [...], "return_value": "done"}
```

机制(端到端):

```
生成器回调 → host 收集 chunks → TASK_INVOKE 响应附 stream_chunks
→ Driver yield 透传 → scheduler 流式分支 → STREAM_EVENT
→ 远程 stream() 逐 token 可见 → 生成器 return 值提交状态
```

## 消费端

```python
for event in graph.stream("run", {}):
    if event.get("eventType") == "messages":
        print(event["payload"]["message"]["content"], end="", flush=True)
```

裸字符串自动包装为 `messages` chunk;也可以显式 yield
`{"type": "custom", "payload": {...}}` 发自定义事件。

## 边界(诚实说明)

- **同步生成器**可携带 return 值(完整结果);
- **async 生成器**因 Python 语法限制不能带值 return,只负责流式、结果为空;
  要"异步模型 + 写状态",用同步生成器包装或走 `agent.stream()`(prebuilt)。

## 运行

```bash
uv run --directory python/reactivegraph python docs/tutorials/code/07_streaming_tokens.py
```

## 进阶

- 非生成器路径也支持流式:`agent.stream()`(prebuilt ReAct agent)按事件
  产出 `messages`;Driver 侧 `TaskExecutor.invokeTask` 亦可返回 async
  generator(TS 绑定层)。
- 想画图:看 `graphToDot`(dot 导出)把任务/事件路由渲染成 Graphviz。
