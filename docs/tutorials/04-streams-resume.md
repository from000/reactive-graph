# 04 · 流式与中断

**目标**:`stream()` 边跑边出事件;`resume()` 支持 human-in-the-loop。

## 前置

```bash
pnpm --filter @reactivegraph/driver build   # 需要 node ≥ 20
```

## stream()

```python
for event in g.stream("run", {"n": 1}):
    ...
```

事件序列里出现 `values`(superstep 后的 state 快照)等类型;final state 收敛正确。底层
driver 有 `StreamMux`(committed/transient 分通道 + cursor),Python 侧以事件形式透出。

## resume()

`resume(run_id, interrupt_response)` 需要 `DriverHost`。中断的 run 停在 checkpoint
处;恢复时携带预期状态版本,stale resume(版本不一致)会被拒绝。

## 运行

```bash
uv run --directory python/reactivegraph python docs/tutorials/code/04_streams_resume.py
```

## 进阶

- spec:[streams-and-interrupts.md](../spec/streams-and-interrupts.md)、
  [durable-execution.md](../spec/durable-execution.md)