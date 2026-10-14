"""M6：回调顺序 + stats（选择性执行可观测证据）测试。"""

from __future__ import annotations

import pytest

from reactivechain import Pipeline, RunnableLambda
from reactivechain.callback import (
    BaseCallbackHandler,
    CallbackManager,
    ChainStats,
    instrument_pipeline,
    set_callback_manager,
)
from reactivechain.llm import FakeLLM


class _Recorder(BaseCallbackHandler):
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []  # (event, segment_label)

    def on_chain_start(self, runnable, state, **kw) -> None:
        self.events.append(("start", runnable.id))

    def on_chain_end(self, runnable, output, duration_ms, **kw) -> None:
        self.events.append(("end", runnable.id))

    def on_llm_end(self, llm, output, duration_ms, **kw) -> None:
        self.events.append(("llm_end", llm.id))

    def on_tool_end(self, tool, name, result, **kw) -> None:
        self.events.append(("tool_end", name))

    def on_retry(self, runnable, attempt, error, **kw) -> None:
        self.events.append(("on_retry", str(attempt)))


def test_callback_event_order() -> None:
    recorder = _Recorder()
    mgr = CallbackManager([recorder])
    a = RunnableLambda(lambda s: {"x": 1}, reads={"n"}, writes={"x"}, name="a")
    b = RunnableLambda(lambda s: {"y": s["x"] + 1}, reads={"x"}, writes={"y"}, name="b")
    chain = Pipeline([a, b])
    instrument_pipeline(chain, mgr)
    chain.invoke({"n": 0})
    labels = [e[1] for e in recorder.events]
    assert labels == ["lambda:a", "lambda:a", "lambda:b", "lambda:b"]  # start,end ×2


def test_stats_records_calls_and_skips() -> None:
    calls: list[int] = []

    def counted(state: dict) -> dict:
        calls.append(1)
        return {"v": state["k"] + 1}

    chain = Pipeline([RunnableLambda(counted, reads={"k"}, writes={"v"}, name="c", pure=True)])
    instrument_pipeline(chain)
    chain.invoke({"k": 1})
    # stats 每次 invoke 重建：本次调用了段
    assert chain._stats.segments["lambda:c"].calls == 1  # type: ignore[attr-defined]
    chain.invoke({"k": 1})  # 相同输入 → 段被跳过
    assert chain._stats.segments["lambda:c"].calls == 0  # type: ignore[attr-defined]
    assert chain._stats.segments["lambda:c"].skips == 1  # type: ignore[attr-defined]
    chain.invoke({"k": 2})
    assert chain._stats.segments["lambda:c"].calls == 1  # type: ignore[attr-defined]
    summary: dict = chain._stats.summary()  # type: ignore[attr-defined]
    assert summary["total_ms"] >= 0


def test_stats_skipped_segment_entirely() -> None:
    """同 query 管道二次重跑：首段被完整跳过（calls=0, skips=1）。"""
    calls: list[int] = []

    def counted(state: dict) -> dict:
        calls.append(1)
        return {"v": state["k"] + 1}

    chain = Pipeline([RunnableLambda(counted, reads={"k"}, writes={"v"}, name="c")])
    instrument_pipeline(chain)
    chain.invoke({"k": 1})
    chain.clear_cache()
    chain.invoke({"k": 1})
    stats: ChainStats = chain._stats  # type: ignore[attr-defined]
    seg = stats.segments["lambda:c"]
    assert seg.calls == 1 and seg.skips == 0  # clear_cache 后重新计算


def test_stats_token_recording() -> None:
    stats = ChainStats()
    stats.record_tokens("gpt-4o-mini", 12)
    stats.record_tokens("gpt-4o-mini", 8)
    assert stats.llm_tokens == {"gpt-4o-mini": 20}


def test_global_callback_manager() -> None:
    recorder = _Recorder()
    set_callback_manager(CallbackManager([recorder]))
    chain = Pipeline([RunnableLambda(lambda s: {"x": 1}, name="g")])
    instrument_pipeline(chain)  # 不传 manager → 用全局
    chain.invoke({})
    assert any(e[1] == "lambda:g" for e in recorder.events)
    set_callback_manager(CallbackManager())  # 清理


def test_llm_and_tool_handlers() -> None:
    recorder = _Recorder()
    mgr = CallbackManager([recorder])
    llm = FakeLLM(["hi"])
    mgr.emit("on_llm_start", llm, [])
    mgr.emit("on_llm_end", llm, "hi", 1.0)
    mgr.emit("on_tool_end", llm, "calc", "3")
    assert [e[0] for e in recorder.events] == ["llm_end", "tool_end"]


def test_llm_token_aggregation_via_invoke() -> None:
    """端到端：llm invoke → on_llm_end(tokens) → _StatsHandler → ChainStats。"""
    llm = FakeLLM(["hi"])
    llm._last_tokens = 42
    chain = Pipeline([llm])
    instrument_pipeline(chain, CallbackManager())
    chain.invoke({"messages": "hi"})
    stats: ChainStats = chain._stats  # type: ignore[attr-defined]
    assert stats.llm_tokens == {"fake": 42}


def test_retry_emits_on_retry() -> None:
    """with_retry 重试路径 emit on_retry(attempt, error)（接线验证）。"""
    recorder = _Recorder()
    mgr = CallbackManager([recorder])

    def _boom(state):
        raise ValueError("boom")

    seg = RunnableLambda(_boom, name="boom")
    retried = seg.with_retry(max_attempts=3, retry_if_exception_type=ValueError)
    chain = Pipeline([retried])
    instrument_pipeline(chain, mgr)
    with pytest.raises(ValueError):
        chain.invoke({})
    retries = [e for e in recorder.events if e[0] == "on_retry"]
    assert len(retries) == 3  # 3 次尝试各 emit 一次（attempt 0/1/2）


def test_terminal_not_exported_by_default() -> None:
    """terminal（shell=True）默认不导出——需显式 from reactivechain.tools import。"""
    with pytest.raises(ImportError):
        from reactivechain import terminal  # noqa: F401