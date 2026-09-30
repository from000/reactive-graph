"""可观测与回调（对应 langchain callback / tracing §4.9）。

- `BaseCallbackHandler`：on_chain_* / on_llm_* / on_tool_* / on_retry / on_stream / on_error；
- `CallbackManager`：全局 + 链级注入，导出结构化事件；
- `ChainStats`：选择性执行的可观测证据（每段耗时、跳过段数、调用次数、token 计数）。

stats 挂在 Pipeline 上（`pipeline.stats`），trace 事件经 CallbackManager 输出
（可接 driver CausalTrace/otel，v1 提供事件序列 + duration 度量）。
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass, field
from typing import Any

from .runnable import Pipeline, Runnable, State


class BaseCallbackHandler:
    """回调处理器基类：全部方法默认为空实现，子类按需覆写。"""

    def on_chain_start(self, runnable: Runnable, state: State, **kw: Any) -> None: ...
    def on_chain_end(
        self, runnable: Runnable, output: State, duration_ms: float, **kw: Any
    ) -> None: ...
    def on_chain_error(self, runnable: Runnable, error: Exception, **kw: Any) -> None: ...
    def on_llm_start(self, llm: Runnable, messages: list[dict], **kw: Any) -> None: ...
    def on_llm_end(self, llm: Runnable, output: str, duration_ms: float, **kw: Any) -> None: ...
    def on_tool_start(self, tool: Runnable, name: str, arguments: dict, **kw: Any) -> None: ...
    def on_tool_end(self, tool: Runnable, name: str, result: Any, **kw: Any) -> None: ...
    def on_retry(
        self, runnable: Runnable, attempt: int, error: Exception, **kw: Any
    ) -> None: ...
    def on_stream(self, runnable: Runnable, chunk: dict, **kw: Any) -> None: ...


class CallbackManager:
    """事件分发器：维护 handler 列表，emit 各类事件。"""

    def __init__(self, handlers: list[BaseCallbackHandler] | None = None) -> None:
        self.handlers: list[BaseCallbackHandler] = list(handlers or [])

    def add(self, handler: BaseCallbackHandler) -> CallbackManager:
        self.handlers.append(handler)
        return self

    def emit(self, event: str, *args: Any, **kwargs: Any) -> None:
        for handler in self.handlers:
            method = getattr(handler, event, None)
            if method is not None:
                method(*args, **kwargs)


# 全局默认管理器（组件未显式注入时使用）
_default_manager = CallbackManager()
# 当前执行链的链级 manager（Pipeline.invoke 经 ContextVar 注入，子段
# 组件事件走链级；未在链内时为 None → 回落全局）。
_current_mgr: contextvars.ContextVar[CallbackManager | None] = (
    contextvars.ContextVar("current_mgr", default=None)
)


def get_callback_manager() -> CallbackManager:
    current = _current_mgr.get()
    return current if current is not None else _default_manager


def set_callback_manager(manager: CallbackManager) -> None:
    """替换全局回调管理器（测试中可清零/注入）。"""
    global _default_manager
    _default_manager = manager


@dataclass
class SegmentStat:
    """单段执行统计。"""

    segment_id: str
    calls: int = 0
    skips: int = 0  # 指纹命中（段未被调用）
    total_ms: float = 0.0


@dataclass
class ChainStats:
    """管道执行统计：段级详情 + 汇总。"""

    segments: dict[str, SegmentStat] = field(default_factory=dict)
    started_at: float = 0.0
    finished_at: float = 0.0
    llm_tokens: dict[str, int] = field(default_factory=dict)

    def record_call(self, segment_id: str, duration_ms: float) -> None:
        stat = self.segments.setdefault(segment_id, SegmentStat(segment_id))
        stat.calls += 1
        stat.total_ms += duration_ms

    def record_skip(self, segment_id: str) -> None:
        stat = self.segments.setdefault(segment_id, SegmentStat(segment_id))
        stat.skips += 1

    def record_tokens(self, model: str, tokens: int) -> None:
        self.llm_tokens[model] = self.llm_tokens.get(model, 0) + tokens

    @property
    def total_ms(self) -> float:
        return (self.finished_at - self.started_at) * 1000 if self.finished_at else 0.0

    @property
    def skipped_segments(self) -> int:
        return sum(1 for s in self.segments.values() if s.skips and s.calls == 0)

    def summary(self) -> dict[str, Any]:
        return {
            "total_ms": round(self.total_ms, 3),
            "segments": {
                sid: {
                    "calls": s.calls,
                    "skips": s.skips,
                    "total_ms": round(s.total_ms, 3),
                }
                for sid, s in sorted(self.segments.items())
            },
            "skipped_segments": self.skipped_segments,
            "llm_tokens": dict(self.llm_tokens),
        }


class _StatsHandler(BaseCallbackHandler):
    """把 on_llm_end 的 token 计数聚合进 pipeline 的 ChainStats。

    由 instrument_pipeline 挂到 manager：llm 侧只 emit（不感知 pipeline），
    stats 聚合职责收拢在本 handler，llm 与 ChainStats 无耦合。
    """

    def __init__(self, pipeline: Pipeline) -> None:
        self._pipeline = pipeline

    def on_llm_end(self, llm: Runnable, output: str, duration_ms: float, **kw: Any) -> None:
        stats = getattr(self._pipeline, "_stats", None)
        tokens = kw.get("tokens")
        if stats is not None and tokens:
            stats.record_tokens(getattr(llm, "name", type(llm).__name__), int(tokens))


def instrument_pipeline(pipeline: Pipeline, manager: CallbackManager | None = None) -> Pipeline:
    """开启 Pipeline 的统计与回调（就地装配；返回原管道以便链式调用）。

    统计/缓存/指纹共用 `Pipeline.invoke` 单套执行路径（runnable.py 内联
    钩子），本函数只做开关与装配：置 `_instrumented`、挂回调 manager、
    装 token 聚合 handler。幂等：重复调用不重复装配（避免事件双发）。
    """
    if pipeline._instrumented:
        return pipeline
    mgr = manager or get_callback_manager()
    pipeline._callback_manager = mgr
    pipeline._stats = ChainStats()
    mgr.add(_StatsHandler(pipeline))
    pipeline._instrumented = True
    return pipeline