"""Function/decorator workflow tests (Task 8, Python side)."""

from __future__ import annotations

from reactivegraph.functional import build_function_graph, entrypoint, functask


def test_functask_decorator_with_on() -> None:
    @functask(kind="effect", on=("visit",))
    def greet(input_: dict) -> dict:
        return {"msg": f"hi {input_['name']}"}

    assert greet.id == "greet"
    assert greet.kind == "effect"
    assert greet.on == ("visit",)


def test_entrypoint_plan_and_build() -> None:
    @functask
    def add_one(input_: dict) -> dict:
        return {"n": input_["n"] + 1}

    plan = entrypoint("g", [add_one], [("run", "add_one")])
    assert plan.graph_id == "g"
    assert plan.task_ids == ("add_one",)

    builder = build_function_graph("g", [add_one], [("run", "add_one")])
    gd = builder.build()
    assert gd.id == "g"
    assert gd.route_for("run") == ["add_one"]