import asyncio

import pytest

from reactivegraph.graph import GraphBuilder, ReactiveGraph


@pytest.mark.asyncio
async def test_ainvoke_matches_sync_invoke() -> None:
    def build(b: GraphBuilder) -> None:
        b.task("t", fn=lambda x: {"n": x["n"] + 1}).on("run", "t")

    g = ReactiveGraph.build(build)
    sync_out = g.invoke("run", {"n": 1})
    async_out = await g.ainvoke("run", {"n": 1})
    assert async_out == sync_out == {"n": 2}


@pytest.mark.asyncio
async def test_astream_yields_values_like_stream() -> None:
    def build(b: GraphBuilder) -> None:
        b.task("t", fn=lambda x: {"n": x["n"] + 1}).on("run", "t")

    g = ReactiveGraph.build(build)
    sync_events = list(g.stream("run", {"n": 1}))
    async_events = [e async for e in g.astream("run", {"n": 1})]
    assert {e.get("eventType") for e in async_events} == {e.get("eventType") for e in sync_events}
    async_values = [e for e in async_events if e.get("eventType") == "values"]
    assert async_values[-1]["payload"]["state"]["n"] == 2


@pytest.mark.asyncio
async def test_astream_surfaces_generator_exception() -> None:
    def build(b: GraphBuilder) -> None:
        b.task("t", fn=lambda x: (_ for _ in ()).throw(RuntimeError("boom"))).on("run", "t")

    g = ReactiveGraph.build(build)
    # force the generator to raise when consumed
    async def consume():
        async for _e in g.astream("run", {}):
            pass

    with pytest.raises(RuntimeError, match="boom"):
        await consume()


@pytest.mark.asyncio
async def test_event_loop_not_blocked_by_slow_task() -> None:
    """ainvoke 把同步内核放到 executor:慢任务执行期间,事件循环仍可处理定时器。"""
    import time

    def build(b: GraphBuilder) -> None:
        def slow(x: dict) -> dict:
            time.sleep(0.3)  # noqa: ASYNC109 - 故意同步阻塞,验证不占 event loop
            return {}

        b.task("slow", fn=slow).on("run", "slow")

    g = ReactiveGraph.build(build)
    loop = asyncio.get_running_loop()
    fired_at: list[float] = []
    loop.call_later(0.05, lambda: fired_at.append(loop.time()))  # 0.05s 后触发

    await g.ainvoke("run", {})  # 内部 time.sleep(0.3) 在 executor 线程
    end = loop.time()

    # 定时器必须在 invoke 结束(≈0.3s)之前触发,证明循环未被阻塞
    assert fired_at, "timer did not fire"
    assert fired_at[0] < end
