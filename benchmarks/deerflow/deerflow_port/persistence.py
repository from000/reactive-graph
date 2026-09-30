"""链路 5：线程持久化（checkpoint 落盘 + 重启恢复 + 断点继续）。

对照 deer-flow v2 `persistence/engine.py`（持久化引擎）+ `runtime/checkpointer`
（状态快照/恢复）+ `runtime/store`：会话图 task 带 reads/writes 声明，每次 run
的提交经 durable event log 落盘（sqlite），thread_id 维度隔离各会话；DriverHost
重启（新子进程）后同 db 从 checkpoint 精确恢复线程状态，继续 run 从断点推进。
"""

from __future__ import annotations

from typing import Any

from reactivegraph import GraphBuilder, ReactiveGraph


def build_session_graph(
    host,
    graph_id: str = "df_session",
) -> ReactiveGraph:
    """构造带持久会话状态的图（`acc` 任务：n = n + 1，effect 语义）。"""

    def acc(s: dict[str, Any]) -> dict[str, Any]:
        return {"n": s.get("n", 0) + 1}

    def build(b: GraphBuilder) -> None:
        b.task("acc", fn=acc, kind="effect",
               on=("run",), reads=("n",), writes=("n",))

    return ReactiveGraph.build(build, host=host, graph_id=graph_id)