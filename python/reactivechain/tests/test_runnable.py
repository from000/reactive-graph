"""M1：Runnable 基类 + | 管道 + 段级选择性（指纹跳过）测试。"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from reactivechain import (
    GeneratorRunnable,
    Pipeline,
    ReactiveChainError,
    RunnableLambda,
    RunnablePassthrough,
)


def _inc_x(state: dict) -> dict:
    return {"x": int(state["n"]) + 1}


def _double(state: dict) -> dict:
    return {"y": int(state["x"]) * 2}


def test_pipe_chains_in_order() -> None:
    chain = RunnableLambda(_inc_x, reads={"n"}, writes={"x"}) | RunnableLambda(
        _double, reads={"x"}, writes={"y"}
    )
    out = chain.invoke({"n": 1})
    assert out == {"y": 4}


def test_three_segment_chain() -> None:
    def step3(state: dict) -> dict:
        return {"z": int(state["y"]) + 10}

    chain = (
        RunnableLambda(_inc_x, reads={"n"}, writes={"x"})
        | RunnableLambda(_double, reads={"x"}, writes={"y"})
        | RunnableLambda(step3, reads={"y"}, writes={"z"})
    )
    assert chain.invoke({"n": 2}) == {"z": 16}


def test_missing_read_key_raises_hint() -> None:
    step = RunnableLambda(_double, reads={"x"}, writes={"y"})
    with pytest.raises(ReactiveChainError, match="缺少输入键"):
        step.invoke({"n": 1})


def test_writes_must_be_declared() -> None:
    def bad(state: dict) -> dict:
        return {"undeclared": 1}

    step = RunnableLambda(bad, writes={"ok"})
    with pytest.raises(ReactiveChainError, match="未声明的键"):
        step.invoke({"a": 1})


def test_return_must_be_dict() -> None:
    with pytest.raises(ReactiveChainError, match="必须返回 dict"):
        RunnableLambda(lambda s: 42).invoke({})


def test_fingerprint_skip_skips_unchanged_input() -> None:
    calls: list[int] = []

    def counted(state: dict) -> dict:
        calls.append(1)
        return {"v": int(state["k"]) + 1}

    # 单段直接调用无缓存；包装成 Pipeline 后获得段级指纹跳过。
    # 决策 R1：仅 pure 段跳过（确定性段显式声明）。
    chain = Pipeline([RunnableLambda(counted, reads={"k"}, writes={"v"}, pure=True)])
    chain.invoke({"k": 1})
    chain.invoke({"k": 1})  # 相同输入 → 跳过
    assert len(calls) == 1
    chain.invoke({"k": 2})  # 不同输入 → 执行
    assert len(calls) == 2


def test_skip_propagates_cached_output() -> None:
    calls: list[int] = []
    downstream: list[int] = []

    def a(state: dict) -> dict:
        calls.append(1)
        return {"x": int(state["k"]) + 1}

    def b(state: dict) -> dict:
        downstream.append(1)
        return {"y": int(state["x"]) * 2}

    chain = RunnableLambda(a, reads={"k"}, writes={"x"}, pure=True) | RunnableLambda(
        b, reads={"x"}, writes={"y"}, pure=True
    )
    first = chain.invoke({"k": 1})
    second = chain.invoke({"k": 1})
    assert first == second == {"y": 4}
    assert len(calls) == 1 and len(downstream) == 1


def test_batch_shared_cache() -> None:
    calls: list[int] = []

    def counted(state: dict) -> dict:
        calls.append(1)
        return {"v": int(state["k"]) * 10}

    chain = Pipeline([RunnableLambda(counted, reads={"k"}, writes={"v"}, pure=True)])
    outs = chain.batch([{"k": 1}, {"k": 1}, {"k": 2}])
    assert outs == [{"v": 10}, {"v": 10}, {"v": 20}]
    assert len(calls) == 2  # 批内重复输入只算一次


def test_passthrough() -> None:
    pt = RunnablePassthrough()
    assert pt.invoke({"a": 1}) == {"a": 1}


class _TokenGen(GeneratorRunnable):
    """生成器段：逐词产出、最终返回 dict。"""

    def generate(self, state: dict) -> Iterator[str]:
        yield from str(state["text"]).split()
        return {"done": True}


def test_generator_segment_stream() -> None:
    seg = _TokenGen()
    chunks = list(seg.stream({"text": "hello world"}))
    assert chunks == [{"chunk": "hello"}, {"chunk": "world"}]


def test_generator_segment_invoke_collects_all() -> None:
    seg = _TokenGen()
    out = seg.invoke({"text": "a b c"})
    assert out == {"done": True}


def test_to_graph_invokes_driver() -> None:
    chain = RunnableLambda(_inc_x, reads={"n"}, writes={"x"}) | RunnableLambda(
        _double, reads={"x"}, writes={"y"}
    )
    graph = chain.to_graph(graph_id="m1_chain")
    out = graph.invoke("run", {"n": 5})
    assert out["y"] == 12


def test_to_graph_segment_level_chain() -> None:
    """A：段级多 task 图——链式累积，输出含中间段写键（区别于单 task 包装）。"""
    chain = RunnableLambda(_inc_x, reads={"n"}, writes={"x"}) | RunnableLambda(
        _double, reads={"x"}, writes={"y"}
    )
    graph = chain.to_graph(graph_id="m1_seg")
    out = graph.invoke("run", {"n": 5})
    assert out["x"] == 6  # 中间段写键出现在输出 → 段级映射生效
    assert out["y"] == 12
    assert len(graph.definition.tasks) == 2  # 每段一个 task


def test_to_graph_pure_segments_marked() -> None:
    """A：pure_segments 指定段 → wire spec 中 kind=='pure'（引擎选择性入口）。"""
    chain = RunnableLambda(_inc_x, reads={"n"}, writes={"x"}) | RunnableLambda(
        _double, reads={"x"}, writes={"y"}
    )
    graph = chain.to_graph(graph_id="m1_pure", pure_segments={"seg_0"})
    spec = graph._to_wire_spec()
    kinds = {t["id"]: t["kind"] for t in spec["tasks"]}
    assert kinds["seg_0"] == "pure"
    assert kinds["seg_1"] == "effect"


# -- 审查修复回归（C1/C2/C3/I1/I2） ----------------------------------------


class _CountingGen(GeneratorRunnable):
    """计数生成器段：记录 generate 被调用的次数。"""

    def __init__(self) -> None:
        self.calls = 0

    def generate(self, state: dict) -> Iterator[str]:
        self.calls += 1
        yield from str(state["text"]).split()
        return {"done": True}


def test_pipeline_stream_does_not_double_execute() -> None:
    """C1：Pipeline.stream 对流式段只执行一次（不二次 invoke）。"""
    seg = _CountingGen()
    chain = Pipeline([seg])
    chunks = list(chain.stream({"text": "a b"}, mode="messages"))  # P3-1：chunk 流 = messages 模式
    assert chunks == [{"chunk": "a"}, {"chunk": "b"}]
    assert seg.calls == 1  # 双重执行会得到 2


def test_pipeline_stream_propagates_final_output() -> None:
    """C1：流式段最终输出（StopIteration.value）传播给后段。"""
    gen = _CountingGen()

    def reader(state: dict) -> dict:
        return {"read": state["done"]}

    chain = Pipeline([gen, RunnableLambda(reader, reads={"done"}, writes={"read"})])
    # 若 stream 不传播 final（last_out={}），reader 会因缺 "done" 抛
    # "缺少输入键" Hint 错误——不抛即传播成功。
    chunks = list(chain.stream({"text": "x"}, mode="messages"))  # P3-1：chunk 流 = messages 模式
    assert chunks == [{"chunk": "x"}]
    assert chain.invoke({"text": "x"}) == {"read": True}


def test_same_name_segments_do_not_share_cache() -> None:
    """C3：同名段（如多个 `<lambda>`）缓存键隔离，不互相污染。"""

    def first(state: dict) -> dict:
        return {"a": state["v"]}

    def second(state: dict) -> dict:
        return {"b": state["v"]}

    chain = Pipeline(
        [
            RunnableLambda(first, name="dup", reads={"v"}, writes={"a"}),
            RunnableLambda(second, name="dup", reads={"v"}, writes={"b"}),
        ]
    )
    out = chain.invoke({"v": 7})
    assert out == {"b": 7}  # 若缓存污染会返回 {"a": ...} 且缺 b


def test_cache_not_polluted_by_caller_mutation() -> None:
    """I2：调用方修改返回 dict 不污染缓存。"""
    calls: list[int] = []

    def build(state: dict) -> dict:
        calls.append(1)
        return {"items": [state["k"]]}

    chain = Pipeline([RunnableLambda(build, reads={"k"}, writes={"items"}, pure=True)])
    out1 = chain.invoke({"k": 1})
    out1["items"].append(999)  # 原地修改返回结果
    out2 = chain.invoke({"k": 1})
    assert out2 == {"items": [1]}  # 缓存未被污染
    assert len(calls) == 1


def test_non_json_input_is_not_cached() -> None:
    """I1：含不可 JSON 序列化值（对象）的输入不缓存，每次执行。"""
    calls: list[int] = []

    def use(state: dict) -> dict:
        calls.append(1)
        return {"ok": state["obj"] is not None}

    chain = Pipeline([RunnableLambda(use, reads={"obj"}, writes={"ok"}, pure=True)])
    marker = object()
    chain.invoke({"obj": marker})
    chain.invoke({"obj": marker})
    assert len(calls) == 2  # 非 JSON 值 → 不可缓存 → 每次都执行


def test_exports_cover_review_gaps() -> None:
    """审查反馈：新符号可从包顶层导入。"""
    import reactivechain

    assert reactivechain.RetryWithErrorOutputParser is not None
    assert reactivechain.messages_to_api is not None
    assert reactivechain.SegmentStat is not None


def test_empty_pipeline_rejected() -> None:
    with pytest.raises(ReactiveChainError, match="至少需要一段"):
        Pipeline([])


def test_generator_scalar_final_normalized_in_stream() -> None:
    """复核反馈：标量 final 在 stream 路径归一化为 {"output": ...}（与 invoke 一致）。"""

    class _ScalarGen(GeneratorRunnable):
        def generate(self, state: dict) -> Iterator[str]:
            yield "块"
            return "最终文本"

    def reader(state: dict) -> dict:
        return {"got": state["output"]}

    chain = Pipeline([_ScalarGen(), RunnableLambda(reader, reads={"output"}, writes={"got"})])
    chunks = list(chain.stream({"text": "x"}, mode="messages"))  # 标量 final 丢失则 reader 缺键
    assert chunks == [{"chunk": "块"}]
    assert chain.invoke({"text": "x"}) == {"got": "最终文本"}


def test_async_methods_match_sync() -> None:
    """B：ainvoke/astream/abatch 输出与同步路径一致。"""
    import asyncio

    chain = RunnableLambda(_inc_x, reads={"n"}, writes={"x"}) | RunnableLambda(
        _double, reads={"x"}, writes={"y"}
    )

    async def run() -> tuple:
        a = await chain.ainvoke({"n": 3})
        chunks = [c async for c in chain.astream({"n": 3}, mode="messages")]
        batch = await chain.abatch([{"n": 1}, {"n": 2}])
        return a, chunks, batch

    out, chunks, batch = asyncio.run(run())
    assert out == {"y": 8}
    assert chunks == []  # messages 模式：无覆写 stream 的段时不产出中间块
    assert batch == [{"y": 4}, {"y": 6}]


def test_async_parallel_invoke_cache_safe() -> None:
    """B：并发 ainvoke 线程安全——结果正确、缓存 key 唯一、无异常。

    注意：段级缓存语义是"key→最近一次输入指纹"（与 ReactiveGraph pure
    跳过一致）——交替输入会互相失效，故不断言执行次数。
    """
    import asyncio

    chain = Pipeline(
        [RunnableLambda(lambda s: {"v": s["k"] * 10}, reads={"k"}, writes={"v"}, pure=True)]
    )

    async def run() -> list:
        return await asyncio.gather(*[chain.ainvoke({"k": i % 2}) for i in range(8)])

    outs = asyncio.run(run())
    assert sorted({o["v"] for o in outs}) == [0, 10]  # 结果正确、无串扰
    assert len(chain._compiled_graph._fallback_cache) == 1  # 单段缓存项唯一
    assert chain.invoke({"k": 0}) == {"v": 0}
    assert chain.invoke({"k": 1}) == {"v": 10}


def test_invoke_runs_via_engine() -> None:
    """方案 C P0：Pipeline.invoke 经引擎执行（单一执行路径断言）。

    - 首次 invoke 惰性编译并缓存图对象（_compiled_graph 就位）；
    - 引擎 fallback 的 pure 指纹缓存被填充（非链侧自建缓存）；
    - 输出仍为链 API 语义（末段实际输出）。
    """
    chain = Pipeline(
        [RunnableLambda(lambda s: {"y": s["k"] * 2}, reads={"k"}, writes={"y"}, pure=True)]
    )
    assert chain.invoke({"k": 3}) == {"y": 6}
    assert chain._compiled_graph is not None
    assert len(chain._compiled_graph._fallback_cache) == 1
    assert chain._compiled_graph.definition.tasks[0].kind == "pure"


def test_to_graph_computed_segments() -> None:
    """P1-2：to_graph(computed_segments) 把单写键 pure 段编译为 computed 派生。"""
    calls: list[int] = []

    def derive(state: dict) -> dict:
        calls.append(1)
        return {"total": state["a"] + state["b"]}

    seg = RunnableLambda(derive, reads={"a", "b"}, writes={"total"}, pure=True)
    chain = Pipeline([seg])
    graph = chain.to_graph(computed_segments={"seg_0"})
    assert len(graph.definition.computeds) == 1  # 非 task 节点
    out = graph.invoke("run", {"a": 1, "b": 2})
    assert out["total"] == 3
    # 读路径未变 → computed 读哈希缓存命中，selector 不重算
    graph.invoke("run", {"a": 1, "b": 2})
    assert len(calls) == 1


def test_computed_segment_rejects_multi_write_key() -> None:
    """P1-2：computed 段必须恰好一个写键（单值派生表达式）。"""

    def multi(state: dict) -> dict:
        return {"x": 1, "y": 2}

    seg = RunnableLambda(multi, writes={"x", "y"}, pure=True)
    chain = Pipeline([seg])
    with pytest.raises(ReactiveChainError, match="恰好一个写键"):
        chain.to_graph(computed_segments={"seg_0"})