"""链路 1：lead_agent 多 agent 编排。

对照 deer-flow v2 `agents/lead_agent/agent.py:make_lead_agent`（create_agent +
middleware + 工具集的多 agent 编排），本模块以声明式图等价实现：
三阶段 agent（planner → executor → verifier）经事件订阅（`<task>:written`）
链式路由，pure 任务带 reads/writes 声明——同一张图天然获得选择性执行
（跨 run 同输入指纹跳过，d0183d4 起 Driver 侧持久化）。

用法：
    g = build_lead_agent(host, calls={"planner": 0, ...})
    out = g.invoke("run", {"query": "..."})
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from reactivegraph import GraphBuilder, ReactiveGraph


def build_lead_agent(
    host,
    calls: dict[str, int] | None = None,
    graph_id: str = "df_lead_agent",
) -> ReactiveGraph:
    """构造三阶段 lead_agent 编排图。

    - `calls`：可选计数表（测试注入断言执行次数）。
    - 阶段产物：plan（研究计划）→ draft（执行稿）→ answer（验证后答案）。
    """
    counters: dict[str, int] = calls if calls is not None else {}

    def counted(name: str, fn: Callable[[dict[str, Any]], dict[str, Any]]):
        def wrapped(s: dict[str, Any]) -> dict[str, Any]:
            counters[name] = counters.get(name, 0) + 1
            return fn(s)

        return wrapped

    def planner(s: dict[str, Any]) -> dict[str, Any]:
        return {"plan": f"plan:{s['query']}"}

    def executor(s: dict[str, Any]) -> dict[str, Any]:
        return {"draft": f"draft:{s['plan']}"}

    def verifier(s: dict[str, Any]) -> dict[str, Any]:
        return {"answer": f"verified:{s['draft']}"}

    def build(b: GraphBuilder) -> None:
        b.task("planner", fn=counted("planner", planner), kind="pure",
               on=("run",), reads=("query",), writes=("plan",))
        b.task("executor", fn=counted("executor", executor), kind="pure",
               on=("planner:written",), reads=("plan",), writes=("draft",))
        b.task("verifier", fn=counted("verifier", verifier), kind="pure",
               on=("executor:written",), reads=("draft",), writes=("answer",))

    return ReactiveGraph.build(build, host=host, graph_id=graph_id)
