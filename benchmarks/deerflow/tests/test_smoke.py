"""smoke：套件锚点自检——DriverHost 真子进程可起、最小图可执行。"""

from __future__ import annotations

from reactivegraph import DriverHost, ReactiveGraph


def test_df_smoke_host_and_minimal_graph(df_host: DriverHost) -> None:
    """DriverHost 握手 + 最小声明式图 invoke（RGP/1 真子进程执行体就绪）。"""

    def build(b) -> None:
        b.task("greet", kind="effect",
               fn=lambda s: {"msg": f"hi {s['name']}"},
               on=("visit",), reads=("name",), writes=("msg",))

    g = ReactiveGraph.build(build, host=df_host, graph_id="df_smoke")
    out = g.invoke("visit", {"name": "Ada"})
    assert out["msg"] == "hi Ada"
