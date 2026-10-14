# 03 · computed 与 scope

**目标**:派生状态按读集缓存/失效;子状态用 scope 命名空间隔离。

## computed

```python
b.computed("total", total_selector, reads=("a", "b"))
```

- `selector(state)` 计算派生值;`reads` 声明它依赖哪些路径
- `selector` 只调用一次,之后命中缓存;**读集路径变化才失效重算**
- 这就是选择性执行在派生状态上的体现(局部重算,而非整图重算)

## scope

```python
b.scope("user")   # 子状态命名空间声明
```

- 任务读写落在 `state[scope]` 下,防止字段名冲突
- 本教程的 Python 侧展示声明;实际读写隔离由 Driver 调度层消费

## 运行

```bash
uv run --directory python/reactivegraph python docs/tutorials/code/03_computed_scope.py
```

## 进阶

- spec:[state-and-transactions.md](../spec/state-and-transactions.md)、
  [native-api.md](../spec/native-api.md)