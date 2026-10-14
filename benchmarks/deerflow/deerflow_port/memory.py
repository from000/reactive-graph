"""链路 3：长期记忆（Driver LongTermStore 持久，跨 run/线程级）。

对照 deer-flow v2 memory_middleware（记忆注入 agent 上下文）+ `runtime/store`
（长期存储）：agent 图内含记忆任务闭环——

- `load_mem`（pure）：读 Driver store（namespace `["df_mem", mem_key]`）注入
  `memory.facts` 到 state；
- `answer`（pure）：有记忆则回用（`recalled:...`），无记忆则 `no-memory`；
- `save_mem`（effect）：run 末尾把本轮 `learned` 事实写回 store（持久）。

隔离语义：长期记忆按 `mem_key` 命名空间隔离（线程/会话维度），与链路 5 的
checkpoint `thread_id`（执行状态维度）正交——这正是 deer-flow 长期记忆 vs
会话状态的区分。
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from reactivegraph import GraphBuilder, ReactiveGraph

_MEM_NS = ["df_mem"]


def build_memory_agent(
    host,
    calls: dict[str, int] | None = None,
    graph_id: str = "df_memory",
) -> ReactiveGraph:
    """构造带长期记忆闭环的 agent 图。

    - `calls`：可选计数表（load/answer/save）。
    - 记忆存取走 graph 的 store_put/store_get（任务内调用，线程安全）。
    """
    counters: dict[str, int] = calls if calls is not None else {}
    holder: dict[str, Any] = {}
    _lock = threading.Lock()

    def counted(name: str, fn: Callable[[dict[str, Any]], dict[str, Any]]):
        def wrapped(s: dict[str, Any]) -> dict[str, Any]:
            with _lock:
                counters[name] = counters.get(name, 0) + 1
            return fn(s)

        return wrapped

    def mem_get(key: str) -> dict[str, Any]:
        raw = holder["g"].store_get(_MEM_NS + [key], "history")
        return raw if isinstance(raw, dict) else {}

    def mem_put(key: str, value: dict[str, Any]) -> None:
        holder["g"].store_put(_MEM_NS + [key], "history", value)

    def load_mem(s: dict[str, Any]) -> dict[str, Any]:
        facts = list(mem_get(s["mem_key"]).get("facts", []))
        return {"memory_facts": facts}

    def answer(s: dict[str, Any]) -> dict[str, Any]:
        facts = s.get("memory_facts", [])
        if facts:
            return {"out": f"recalled:{','.join(facts)}"}
        return {"out": "no-memory"}

    def save_mem(s: dict[str, Any]) -> dict[str, Any]:
        # 本轮事实并入历史（持久化写回，memory middleware 语义）
        prev = mem_get(s["mem_key"]).get("facts", [])
        learned = s.get("learned", [])
        merged = list(prev)
        for f in learned:
            if f not in merged:
                merged.append(f)
        mem_put(s["mem_key"], {"facts": merged})
        return {}

    def build(b: GraphBuilder) -> None:
        # ``opaque``, not ``pure``: this task reads the Driver's long-term
        # store (``store_get``), whose contents are not part of the graph state
        # and therefore cannot be covered by a ``reads`` fingerprint. A pure
        # task would be skipped when only ``mem_key`` is unchanged — even
        # though a previous run wrote new facts into the store — so the second
        # invoke would silently see stale (empty) memory.
        b.task("load_mem", fn=counted("load", load_mem), kind="opaque",
               on=("run",), reads=("mem_key",), writes=("memory_facts",))
        b.task("answer", fn=counted("answer", answer), kind="pure",
               on=("load_mem:written",), reads=("memory_facts",), writes=("out",))
        b.task("save_mem", fn=counted("save", save_mem), kind="effect",
               on=("answer:written",), reads=("mem_key", "learned"), writes=())

    g = ReactiveGraph.build(build, host=host, graph_id=graph_id)
    holder["g"] = g
    return g