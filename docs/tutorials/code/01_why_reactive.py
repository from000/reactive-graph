"""01 · 为什么反应式 —— 最小图,相同输入输出幂等一致。

fingerprint skip 是 Driver 的调度行为:用 DriverHost(见 04/06)时,
相同事件+相同输入会跳过 handler;本文件用 fallback 模式,保证任何环境可跑。
"""

from reactivegraph import GraphBuilder, ReactiveGraph


def build(b: GraphBuilder) -> None:
    b.task("greet", kind="pure", fn=lambda x: {"msg": f"hi {x.get('name')}"}).on("visit", "greet")


g = ReactiveGraph.build(build)

first = g.invoke("visit", {"name": "Ada"})
second = g.invoke("visit", {"name": "Ada"})  # 相同事件 + 相同输入

assert first == second == {"name": "Ada", "msg": "hi Ada"}
print("state:", first)
print("OK: 相同事件+输入,输出幂等一致。")
print("提示: DriverHost 下第二次会 fingerprint_unchanged 跳过(见 06)。")