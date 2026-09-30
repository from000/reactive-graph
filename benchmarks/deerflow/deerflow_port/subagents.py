"""链路 2：sub-agents 派发执行（并行 + 容量控制）。

对照 deer-flow v2 `subagents/executor.py`（执行编排）、`subagents/registry.py`
（内置 sub-agent 类型注册）、`subagents/capacity.py`（并发/总量上限）：
主编排任务（dispatcher）将 batch items 分派给 `capacity` 个并行 sub-agent 任务
（引擎事件订阅天然并行执行），超额 items 截断并计数（active/dropped）。

用法：
    g = build_subagent_dispatcher(host, calls={"sub0": 0, ...}, n_items=5, capacity=5)
    out = g.invoke("run", {"items": [...]})
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from reactivegraph import GraphBuilder, ReactiveGraph


def build_subagent_dispatcher(
    host,
    calls: dict[str, int] | None = None,
    n_items: int = 5,
    capacity: int = 5,
    graph_id: str = "df_subagents",
) -> ReactiveGraph:
    """构造 sub-agent 派发图。

    - dispatcher（effect）：按 capacity 截断 items，写 active/dropped 计数。
    - sub{i}（pure）：订阅 dispatcher 完成事件，并行处理对应 item（容量内）。
    """
    counters: dict[str, int] = calls if calls is not None else {}

    def dispatcher(s: dict[str, Any]) -> dict[str, Any]:
        kept = s["items"][:capacity]
        return {"active": len(kept), "dropped": len(s["items"]) - len(kept)}

    def make_sub(i: int) -> Callable[[dict[str, Any]], dict[str, Any]]:
        def fn(s: dict[str, Any]) -> dict[str, Any]:
            counters[f"sub{i}"] = counters.get(f"sub{i}", 0) + 1
            return {f"result_{i}": f"done:{s['items'][i]}"}

        return fn

    def build(b: GraphBuilder) -> None:
        b.task("dispatcher", fn=dispatcher, kind="effect",
               on=("run",), reads=("items",), writes=("active", "dropped"))
        for i in range(min(n_items, capacity)):
            b.task(f"sub{i}", fn=make_sub(i), kind="pure",
                   on=("dispatcher:written",), reads=("items",),
                   writes=(f"result_{i}",))

    return ReactiveGraph.build(build, host=host, graph_id=graph_id)