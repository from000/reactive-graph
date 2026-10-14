# 08 · 真实 LLM token 流式(零第三方依赖)

**目标**:用**标准库**对接任意 OpenAI 兼容网关,逐 token 流式输出——可复用的最小
模式(AGNES 网关已实测)。

## 前置

```bash
export AGNES_API_KEY=...                          # 或你的网关 key
export CUSTOM_LLM_URL=https://apihub.agnes-ai.com/v1
export CUSTOM_MODEL=agnes-2.5-flash
```

## 最小模式(标准库)

```python
from urllib import request

req = request.Request(
    f"{base}/chat/completions",
    data=json.dumps({"model": model, "messages": messages, "stream": True}).encode(),
    headers={"Content-Type": "application/json",
             "Authorization": f"Bearer {key}",
             "Accept": "text/event-stream"},
)
with request.urlopen(req, timeout=120) as r:
    for raw in r:                        # 逐行 SSE
        line = raw.decode().strip()
        if not line.startswith("data:"): continue
        data = line[5:].strip()
        if data == "[DONE]": break
        delta = json.loads(data)["choices"][0].get("delta") or {}
        if delta.get("content"):
            yield delta["content"]       # 一个 token
```

要点:
- `stream: true` + `Accept: text/event-stream` → 网关回 SSE;
- 每行 `data: {json}` 里 `choices[0].delta.content` 是一个 token,`data: [DONE]` 结束;
- 全程标准库,不装 `openai` SDK。

## 与工作台的关系

- `examples/deerflow/frontend/server.py` 里 `agnes_stream()` 就是本模式的生产版
  (外加 tool_calls 双向规范化);
- 工作台对话页勾选「真实模型」即用 `EventSource` 逐 token 打字机;
- `POST /api/agent-real` 是真实 ReAct agent 循环(AGNES function calling)。

## 运行

```bash
uv run --directory python/reactivegraph python docs/tutorials/code/08_real_llm_streaming.py \
  "用一句话介绍 ReactiveGraph"
```

## 进阶

- 想给 agent 加工具:看 `docs/tutorials/code/07_streaming_tokens.py`(生成器回调进
  图)+ 工作台的 `agent-real`(工具调用全链路);
- 无 key 时脚本打印配置说明并正常退出(教程检查不失败);有 key 时真实流式。
