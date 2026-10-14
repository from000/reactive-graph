# DeerFlow 风格前端工作台 — 设计文档

日期:2026-09-10 · 状态:已批准(用户确认:纯 Python 单页 / 对话式+能力实验室 / 尽量含浏览器端到端)

## 目标

为 `examples/deerflow` 增加一个 DeerFlow 风格的前端工作台(纯 Python 单页、零构建),
把 ReactiveGraph **已实现的能力面完整端到端验证一遍**。前端只是消费者,所有能力
走引擎真实实现(真实 Node Driver)。

诚实边界:deer-flow 官方前端源码当前不可达(404,已核实),本设计做"DeerFlow 风格"
而非像素复刻;浏览器端到端受本机环境影响,不可用时声明 unverified,后端功能验证不受影响。

## 架构

```
浏览器(index.html / app.js / style.css)
  ↕ HTTP REST + SSE(EventSource,原生 JS)
examples/deerflow/frontend/server.py   ← 仅标准库 http.server,零第三方依赖
  ↕ DriverHost(Node driver,RGP/1)
ReactiveGraph 引擎(全能力真实实现)
```

- 新增目录:`examples/deerflow/frontend/`(server.py、index.html、app.js、style.css)
- 新增测试:`examples/deerflow/tests/test_frontend.py`

## 能力端点(12 项,全真不 mock)

| 端点 | 能力 |
|---|---|
| `POST /api/invoke` | 事件路由 + 并行任务 + state 合并 |
| `GET /api/stream?q=`(SSE) | values 快照 + 生成器 token 流(messages 打字机) |
| `GET /api/computed` | computed 派生值 |
| `GET /api/scope?tenant=A/B` | scope 多租户隔离 |
| `POST /api/checkpoint` / `GET /api/checkpoints` | checkpoint 持久化 + 列表 |
| `POST /api/restore` | time travel restore_thread(回滚+分支) |
| `POST /api/resume` | human-in-the-loop 中断恢复 |
| `POST /api/agent` | prebuilt ReAct agent + ToolNode |
| `POST/GET /api/store` | long-term store put/get |
| `GET /api/dot` | graphToDot 渲染 DOT(`<pre>` 展示) |
| `POST /api/recursion` | recursion limit 超限演示(RecursionLimitError 带 Hint) |
| `POST /api/vector` | 向量 upsert + cosine search |

## UI(DeerFlow 风格,暗色)

- 标签页 1 **对话**:输入研究问题 → token 打字机 → 右侧状态/事件轨迹
- 标签页 2 **能力实验室**:12 张卡片,点击执行并展示输出/时序
- 原生 JS(`fetch` + `EventSource`)、暗色主题(`#0d1117` 系)、无构建

## 验证

1. `pytest`:`tests/test_frontend.py` 每个端点连真实 Driver 断言
2. `curl` 验证 SSE 流式;`node --check app.js` 验 JS 语法
3. 尽量含浏览器端到端:尝试 playwright + headless;不可用则明确声明 unverified
4. 全量回归:`pnpm test` + Python 全量(不破坏现有 91+ 测试)

## 交付物

- `examples/deerflow/frontend/{server.py,index.html,app.js,style.css}`
- `examples/deerflow/tests/test_frontend.py`
- `examples/deerflow/README.md` 更新(新增"运行前端工作台"一节)
- 独立 commit