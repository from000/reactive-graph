"""03 · computed 与 scope —— 派生状态缓存/失效 + 子状态命名空间声明。

computed.selector 声明 reads:只有读集变化才重算(否则命中缓存),这正是
ReactiveGraph 选择性执行在派生状态上的体现。
"""

from reactivegraph import GraphBuilder


def run_computed() -> None:
    state = {"a": 1, "b": 2}
    calls: list[int] = []

    def total_selector(s: dict) -> int:
        calls.append(1)
        return s["a"] + s["b"]

    b = GraphBuilder("computed_demo")
    b.computed("total", total_selector, reads=("a", "b"))

    c = b.build()._computed_by_id["total"]
    assert c.evaluate(state) == 3
    assert c.evaluate(state) == 3  # 命中缓存,不调用 selector
    assert len(calls) == 1

    state["a"] = 10  # 读集变化 → 失效重算
    assert c.evaluate(state) == 12
    assert len(calls) == 2
    print("OK: computed 按读集缓存/失效(a+b =", c.evaluate(state), ")")


def run_scope() -> None:
    b = GraphBuilder("scope_demo").scope("user")  # 子状态命名空间声明
    assert b.build().scopes == ["user"]
    print("OK: scope 声明记录:", b.build().scopes)


if __name__ == "__main__":
    run_computed()
    run_scope()