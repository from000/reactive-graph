"""P3-1 Task 2：chain.stream 统一 mode=values|updates|messages。

- messages（默认）：向后兼容的 LLM/生成器段原始 chunk（{"chunk": ...}）
- values：引擎事件流的全 state 快照帧
- updates：段写键增量 {seg_id: writes}
"""

from __future__ import annotations

import pytest

from reactivechain import Pipeline, ReactiveChainError, RunnableLambda


def test_stream_values_updates_modes() -> None:
    """values 末帧 = 最终 state；updates = 段写键增量。"""
    chain = Pipeline([
        RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"}, pure=True)
    ])
    vals = list(chain.stream({"n": 1}, mode="values"))
    assert vals[-1]["x"] == 2  # values：全 state 快照，末帧含最终结果
    updates = list(chain.stream({"n": 1}, mode="updates"))
    assert updates[-1] == {"seg_0": {"x": 2}}  # 段写键增量


def test_stream_updates_multi_segment_increment() -> None:
    """updates：每段一个增量帧，顺序 = 段顺序。"""
    chain = Pipeline([
        RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"}, pure=True),
        RunnableLambda(lambda s: {"y": s["x"] * 2}, reads={"x"}, writes={"y"}, pure=True),
    ])
    updates = list(chain.stream({"n": 1}, mode="updates"))
    assert updates[0] == {"seg_0": {"x": 2}}
    assert updates[-1] == {"seg_1": {"y": 4}}


def test_stream_messages_mode_keeps_chunk_shape() -> None:
    """messages：LLM/生成器段原始 chunk 形状不变（C1 回归）。"""
    from reactivechain import GeneratorRunnable

    class _Gen(GeneratorRunnable):
        def __init__(self) -> None:
            self.calls = 0

        def generate(self, state: dict):
            self.calls += 1
            yield "a"
            yield "b"
            return {"done": True}

    gen = _Gen()
    chain = Pipeline([gen])
    assert list(chain.stream({}, mode="messages")) == [{"chunk": "a"}, {"chunk": "b"}]
    assert gen.calls == 1  # 不双重执行（C1）


def test_stream_values_multi_frame() -> None:
    """values：多段链产出中间帧，末帧 = 最终 state（P3-1 Task 4 同构前置）。"""
    chain = Pipeline([
        RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"}, pure=True),
        RunnableLambda(lambda s: {"y": s["x"] * 2}, reads={"x"}, writes={"y"}, pure=True),
    ])
    frames = list(chain.stream({"n": 1}, mode="values"))
    assert frames[0] == {"n": 1}  # 初始帧 = 输入 state（对齐 Driver 首帧）
    assert frames[1]["x"] == 2  # 段 0 写后帧
    assert frames[-1]["x"] == 2 and frames[-1]["y"] == 4  # 末帧 = 最终 state


def test_stream_astream_mode_passthrough() -> None:
    """astream(mode=...) 透传：异步路径与同步路径输出一致。"""
    import asyncio

    chain = Pipeline([
        RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"}, pure=True)
    ])

    async def run() -> list[dict]:
        return [e async for e in chain.astream({"n": 1}, mode="updates")]

    assert asyncio.run(run()) == list(chain.stream({"n": 1}, mode="updates"))


def test_stream_unknown_mode_raises() -> None:
    chain = Pipeline([RunnableLambda(lambda s: {"x": 1}, writes={"x"})])
    with pytest.raises(ReactiveChainError, match="unknown stream mode"):
        list(chain.stream({}, mode="bogus"))


def test_stream_values_error_fallback_raises() -> None:
    """P3-1 Task 4：fallback 链路径段异常 → chain.stream 抛错不吞。"""
    def boom(s: dict) -> dict:
        raise RuntimeError("boom")

    chain = Pipeline([RunnableLambda(boom, writes={"x"})])
    with pytest.raises(RuntimeError, match="boom"):
        list(chain.stream({"n": 1}, mode="values"))