"""错误消息必须带"可能原因 + 修法"(Hint)——DX 要求。"""

import pytest

from reactivegraph.graph import GraphBuilder, GraphBuildError, ReactiveGraph


def test_no_route_error_carries_hint() -> None:
    def build(b: GraphBuilder) -> None:
        b.task("t", fn=lambda x: {})

    g = ReactiveGraph.build(build)
    with pytest.raises(GraphBuildError) as ei:
        g.invoke("missing", {})
    assert "Hint:" in str(ei.value)


def test_unknown_route_target_carries_hint() -> None:
    with pytest.raises(GraphBuildError) as ei:
        GraphBuilder("g").on("e", "nope")
    assert "Hint:" in str(ei.value)


def test_duplicate_task_id_carries_hint() -> None:
    with pytest.raises(GraphBuildError) as ei:
        GraphBuilder("g").task("t").task("t")
    assert "Hint:" in str(ei.value)


def test_resume_requires_host_carries_hint() -> None:
    def build(b: GraphBuilder) -> None:
        b.task("t", fn=lambda x: {})

    g = ReactiveGraph.build(build)
    with pytest.raises(GraphBuildError) as ei:
        g.resume("run-1", {})
    assert "Hint:" in str(ei.value)