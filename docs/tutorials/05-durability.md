# 05 · 持久化

**目标**:thread checkpoint 可见;long-term store 读写/搜索/命名空间。

## checkpoint

```python
g.invoke("run", {})            # 默认 thread
g.get_state()                  # 当前 thread 状态快照
g.list_checkpoints()           # 历史 checkpoint 列表
```

## long-term store

```python
g.store_put(["users", "alice"], "prefs", {"theme": "dark"})
g.store_get(["users", "alice"], "prefs")            # -> {"theme": "dark"}
g.store_search(["users"], filter_={"theme": "dark"}) # 前缀 namespace + 过滤
g.list_namespaces()
```

- store 跨 thread/进程共享;checkpoint 支持 sqlite/postgres/redis 后端
- 底层:事件日志 + 定时快照 + compact(见 spec),恢复时 receipts 幂等重放

## 运行

```bash
uv run --directory python/reactivegraph python docs/tutorials/code/05_durability.py
```

## 进阶

- spec:[durable-execution.md](../spec/durable-execution.md)