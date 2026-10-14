"""02 · 第一个图 —— task + on + invoke 五步上手。

重要模型:同一次事件路由的多个任务**各自读 payload、并行写不同字段**;
任务间要共享派生值,用 computed 读 state(见 03),不是任务调用链。
"""

from reactivegraph import GraphBuilder, ReactiveGraph


def build(b: GraphBuilder) -> None:
    b.task("parse", fn=lambda x: {"name": x["raw"].strip().title()}).on("go", "parse")
    b.task("audit", fn=lambda x: {"seen": x["raw"]}).on("go", "audit")


g = ReactiveGraph.build(build)
out = g.invoke("go", {"raw": "  ada  "})

assert out == {"raw": "  ada  ", "name": "Ada", "seen": "  ada  "}
print("state:", out)
print("OK: 一个事件并行路由到多个任务,输出合并成 state。")