# 02 · 第一个图

**目标**:用 `task` + `on` + `invoke` 跑通第一个图,理解事件路由。

## 核心

- `task(id, fn=...)`:声明任务;`fn` 是**纯函数**:payload → state 增量(dict)
- `.on(event, task_id)`(或 `task(..., on=(event,))`):把事件路由给任务
- `invoke(event, payload)`:一次触发,payload 路由给所有监听任务,**并行写各自字段**

> 模型关键:任务之间通过 **state/computed** 共享派生值(见 03),不是靠任务调用链。

## 运行

```bash
uv run --directory python/reactivegraph python docs/tutorials/code/02_first_graph.py
# state: {'name': 'Ada', 'seen': '  ada  '}
```

## 进阶

- 路由拥有未知任务?`on` 会抛 `GraphBuildError`
- 多事件:`task(..., on=("a", "b"))` —— 一个任务监听多个事件
- spec:[native-api.md](../spec/native-api.md)