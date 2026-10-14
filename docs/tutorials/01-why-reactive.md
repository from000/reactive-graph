# 01 · 为什么反应式

**目标**:理解 ReactiveGraph 与"每次全量重跑"式执行引擎(如 langgraph 的
Pregel)的本质区别——**选择性执行**:相同输入不再重新计算。

## 核心概念

- **任务(task)**:声明式 `id/kind/fn/on`,函数是纯的:输入 → 输出(或 `(输出, 收据)`)
- **事件(event)**:`invoke(event, payload)` 把 payload 路由给 `on(event)` 的任务
- **指纹跳过(fingerprint skip)**:对 `pure` 任务,若本次输入与上次**完全一致**,
  Driver 直接跳过 handler(`skip` 记录),输出沿用上次结果
- **computed**:依赖声明后按读集失效,只重算被改动的派生值

## 为什么这重要

| | 全量重跑 | ReactiveGraph |
|---|---|---|
| 重复相同事件 | 全部节点重算 | 0 个节点执行 |
| 1000 节点改 1 字段 | ~7624 ms(实测) | ~5 ms(实测) |
| no-gain(全依赖链) | 必须全跑 | 也必须全跑,但常数开销低 |

> 实测数字来自 `benchmarks/differential/`(真实 langgraph 1.2.11 vs 原生
> Driver,同图同输入、输出一致断言)。**不要只信 README 的旧数字**——用
> 差分基准复现:见 `benchmarks/differential/README.md`。

## 运行

```bash
uv run --directory python/reactivegraph python docs/tutorials/code/01_why_reactive.py
```

输出里你会看到:同一个图连续 invoke 两次,state 一致(任务幂等)。
**fingerprint skip 是 Driver 的调度行为**——要用 `DriverHost`(见 04)才能观察到
trace 中的 `fingerprint_unchanged` 跳过记录;本教程(01–03)用无 host 的
fallback 模式保证在任何环境可直接跑,skip 的实战演示见 [06](06-1000-nodes.md)。