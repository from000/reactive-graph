# ReactiveGraph Tutorials

从"为什么反应式"到"写出第一个 1000 节点应用"的渐进教程。每篇配**可运行代码**
(`code/` 目录),`scripts/check-tutorials.py` 在 CI 里逐一执行验证。

| 教程 | 内容 | 运行方式 |
|---|---|---|
| [01 为什么反应式](01-why-reactive.md) | 选择性执行 vs 全量重跑 | 纯 Python(无 Driver) |
| [02 第一个图](02-first-graph.md) | `task`/`on`/`invoke` 五步上手 | 纯 Python |
| [03 computed 与 scope](03-computed-scope.md) | 派生状态与子状态命名空间 | 纯 Python |
| [04 流式与中断](04-streams-resume.md) | `stream()` / human-in-the-loop | 需 DriverHost(本地) |
| [05 持久化](05-durability.md) | checkpoint / long-term store | 需 DriverHost(本地) |
| [06 1000 节点应用](06-1000-nodes.md) | fan-out 吞吐 + 选择性执行发生在哪一层 | 需 DriverHost(本地) |

要求:Node ≥ 20(仅 04–06),driver 构建产物存在(见 [toolchain](../toolchain.md))。

## 每篇的结构

- **目标**:这一篇做完你能得到什么
- **代码**:完整可运行,注释即讲解
- **验证**:跑一行的命令
- **进阶**:指向 [spec](../spec/native-api.md) 对应章节

运行任意一篇的代码(01–03):

```bash
uv run --directory python/reactivegraph python docs/tutorials/code/01_why_reactive.py
```