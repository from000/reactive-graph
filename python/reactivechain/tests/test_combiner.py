"""M6：组合器测试——Parallel/Branch/Fallback/Assign/with_retry/map 矩阵。"""

from __future__ import annotations

import pytest

from reactivechain import (
    ReactiveChainError,
    RunnableBranch,
    RunnableFallback,
    RunnableLambda,
    RunnableParallel,
)
from reactivechain.llm import FakeLLM


def _inc(state: dict) -> dict:
    return {"x": state["n"] + 1}


def _double(state: dict) -> dict:
    return {"y": state["n"] * 2}


def test_parallel_merges_branches() -> None:
    par = RunnableParallel(
        {
            "inc": RunnableLambda(_inc, reads={"n"}, writes={"x"}),
            "dbl": RunnableLambda(_double, reads={"n"}, writes={"y"}),
        }
    )
    out = par.invoke({"n": 3})
    assert out["inc"] == {"x": 4}
    assert out["dbl"] == {"y": 6}


def test_parallel_empty_hint() -> None:
    with pytest.raises(ReactiveChainError, match="至少需要一个分支"):
        RunnableParallel()


def test_parallel_in_pipeline() -> None:
    par = RunnableParallel(
        {
            "prompt": RunnableLambda(lambda s: {"p": f"值{s['n']}"}, reads={"n"}, writes={"p"}),
            "lang": RunnableLambda(lambda s: {"l": "中文"}, writes={"l"}),
        }
    )
    chain = par | RunnableLambda(
        lambda s: {"len": len(s["prompt"]["p"]) + len(s["lang"]["l"])},
        reads={"prompt", "lang"},
        writes={"len"},
    )
    out = chain.invoke({"n": 1})
    assert out["len"] == len("值1") + len("中文")


def test_branch_routes_by_predicate() -> None:
    branch = RunnableBranch(
        [(lambda s: s["n"] > 0, RunnableLambda(lambda s: {"sign": "正"}, writes={"sign"}))],
        RunnableLambda(lambda s: {"sign": "非正"}, writes={"sign"}),
    )
    assert branch.invoke({"n": 5}) == {"sign": "正"}
    assert branch.invoke({"n": -1}) == {"sign": "非正"}


def test_branch_predicate_error_hint() -> None:
    def bad_pred(s: dict) -> bool:
        raise ValueError("谓词内部错误")

    branch = RunnableBranch([(bad_pred, FakeLLM(["x"]))], FakeLLM(["y"]))
    with pytest.raises(ReactiveChainError, match="谓词异常"):
        branch.invoke({"n": 1})


def test_fallback_on_error() -> None:
    def boom(state: dict) -> dict:
        raise ValueError("主链失败")

    fallback = RunnableFallback(
        RunnableLambda(boom, writes={"v"}),
        RunnableLambda(lambda s: {"v": "回退值"}, writes={"v"}),
    )
    assert fallback.invoke({"a": 1}) == {"v": "回退值"}


def test_fallback_all_fail_raises() -> None:
    def boom(state: dict) -> dict:
        raise ValueError("总是失败")

    fallback = RunnableFallback(
        RunnableLambda(boom, writes={"v"}),
        RunnableLambda(boom, writes={"v"}),
    )
    with pytest.raises(ValueError):
        fallback.invoke({})


def test_with_fallbacks_method() -> None:
    def boom(state: dict) -> dict:
        raise RuntimeError("挂")

    chain = RunnableLambda(boom, writes={"v"}).with_fallbacks(
        [RunnableLambda(lambda s: {"v": 42}, writes={"v"})]
    )
    assert chain.invoke({}) == {"v": 42}


def test_with_retry_succeeds() -> None:
    calls: list[int] = []

    def flaky(state: dict) -> dict:
        calls.append(1)
        if len(calls) < 3:
            raise ValueError("再试")
        return {"v": "好"}

    chain = RunnableLambda(flaky, writes={"v"}).with_retry(
        max_attempts=3, retry_if_exception_type=ValueError
    )
    assert chain.invoke({}) == {"v": "好"}
    assert len(calls) == 3


def test_with_retry_stops_on_unexpected_error() -> None:
    def type_error(state: dict) -> dict:
        raise TypeError("不重试的类型")

    chain = RunnableLambda(type_error, writes={"v"}).with_retry(
        max_attempts=3, retry_if_exception_type=ValueError
    )
    with pytest.raises(TypeError):
        chain.invoke({})


def test_assign_injects_fields() -> None:
    base = RunnableLambda(lambda s: {"out": "基础"}, writes={"out"})
    assigned = base.assign(extra=RunnableLambda(lambda s: {"extra": "注入"}, writes={"extra"}))
    out = assigned.invoke({"seed": 1})
    assert out["out"] == "基础"
    assert out["extra"] == "注入"


def test_map_batch() -> None:
    step = RunnableLambda(lambda s: {"v": s["k"] * 2}, reads={"k"}, writes={"v"})
    mapper = step.map(max_batch_size=2)
    out = mapper.invoke({"items": [{"k": 1}, {"k": 2}, {"k": 3}, {"k": 4}, {"k": 5}]})
    assert out["items_out"] == [{"v": 2}, {"v": 4}, {"v": 6}, {"v": 8}, {"v": 10}]


def test_map_missing_items_hint() -> None:
    step = RunnableLambda(lambda s: s, writes=set())
    with pytest.raises(ReactiveChainError, match="items"):
        step.map().invoke({})


def test_parallel_reads_writes_union() -> None:
    par = RunnableParallel(
        {
            "a": RunnableLambda(lambda s: {}, reads={"k1"}, writes=set()),
            "b": RunnableLambda(lambda s: {}, reads={"k2"}, writes=set()),
        }
    )
    assert par.reads == {"k1", "k2"}
    assert par.writes == {"a", "b"}