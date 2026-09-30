"""P3-3 Task 2：链回调（on_chain_start/end/skip）↔ 引擎 trace 映射断言。

按附录 C：`on_chain_start(seg)` ↔ `task:{tid}:start`；`on_chain_end` ↔
`task:{tid}`；skip（pure 指纹命中）↔ `cache_hit`。链编译段 id = `seg_i`，
引擎 trace 位于 `chain._compiled_graph._trace`（fallback 执行时记录）。
"""

from __future__ import annotations

from reactivechain import Pipeline, RunnableLambda
from reactivechain.callback import BaseCallbackHandler, CallbackManager, instrument_pipeline


class _Recorder(BaseCallbackHandler):
    """记录链级回调序列（start/end）。"""

    def __init__(self) -> None:
        self.starts: list[str] = []
        self.ends: list[str] = []

    def on_chain_start(self, runnable, state, **kw):  # noqa: ANN001
        self.starts.append(runnable.id)

    def on_chain_end(self, runnable, state, duration_ms, **kw):  # noqa: ANN001
        self.ends.append(runnable.id)


def _trace_events(chain: Pipeline) -> list[str]:
    return [t["event"] for t in chain._compiled_graph._trace]


def _make_chain(pure_add: bool = False) -> Pipeline:
    def add_one(s: dict) -> dict:
        return {"x": s["n"] + 1}

    def double(s: dict) -> dict:
        return {"x": s["x"] * 2}

    return Pipeline([
        RunnableLambda(add_one, reads={"n"}, writes={"x"}, pure=pure_add),
        RunnableLambda(double, reads={"x"}, writes={"x"}),
    ])


def test_chain_callbacks_map_to_fallback_trace() -> None:
    """on_chain_start/end 回调序列 ↔ task:seg_i:start / task:seg_i 一一对应。"""
    recorder = _Recorder()
    chain = instrument_pipeline(_make_chain(), CallbackManager([recorder]))
    out = chain.invoke({"n": 1})
    assert out["x"] == 4

    events = _trace_events(chain)
    # 引擎 trace 事件序（串行段）：seg_0:start → seg_0 → seg_1:start → seg_1
    idx = [events.index(e) for e in (
        "task:seg_0:start", "task:seg_0", "task:seg_1:start", "task:seg_1")]
    assert idx == sorted(idx)
    # 回调 ↔ trace 一一对应：每段 start/end 各一，顺序一致（id 名段级 vs
    # 引擎级不同：lambda:add_one ↔ seg_0、lambda:double ↔ seg_1）
    assert recorder.starts == ["lambda:add_one", "lambda:double"]
    assert recorder.ends == ["lambda:add_one", "lambda:double"]
    assert len(recorder.starts) == len([e for e in events if e.endswith(":start")])
    assert len(recorder.ends) == len([e for e in events if not e.endswith(":start")])


def test_chain_skip_maps_to_cache_hit() -> None:
    """pure 段同输入二次 invoke：skip 回调记录 + 引擎 trace 出现 cache_hit。"""
    recorder = _Recorder()
    chain = instrument_pipeline(_make_chain(pure_add=True), CallbackManager([recorder]))
    chain.invoke({"n": 1})
    assert "cache_hit" not in _trace_events(chain)
    assert recorder.starts == ["lambda:add_one", "lambda:double"]

    chain.invoke({"n": 1})  # 同输入 → add_one 指纹命中
    events = _trace_events(chain)
    assert "cache_hit" in events
    # 精确 skip 断言：仅 add_one 被跳过；double（effect）仍执行 → 二次
    # invoke 只新增 double 的 start/end 回调（skip 段不经过 wrapper）。
    skipped = [seg for seg, st in chain._stats.segments.items() if st.skips]
    assert skipped == ["lambda:add_one"]
    assert recorder.starts == ["lambda:add_one", "lambda:double", "lambda:double"]
    assert recorder.ends == ["lambda:add_one", "lambda:double", "lambda:double"]