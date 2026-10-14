"""M5：工具层测试——schema 推导、Toolkit、内置工具、ToolNode 对接。"""

from __future__ import annotations

import os
import shutil
import threading
from enum import Enum
from pathlib import Path
from typing import Literal

import pytest
from pydantic import BaseModel
from reactivegraph import DriverHost
from typing_extensions import TypedDict

from reactivechain import ReactiveChainError
from reactivechain.tools import (
    InjectedSecret,
    InjectedSecretArg,
    InjectedState,
    InjectedToolCallId,
    StructuredTool,
    Toolkit,
    ToolNodeAdapter,
    ToolResult,
    ToolStreamChunk,
    calculator,
    current_date,
    current_time,
    terminal,
    tool,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


class _PydanticItem(BaseModel):
    name: str
    price: float


class _Color(Enum):
    RED = "red"
    BLUE = "blue"


class _Filter(TypedDict):
    age: int
    name: str


def test_tool_decorator_schema_from_annotations() -> None:
    @tool
    def add(a: int, b: int = 0) -> int:
        """两数相加。"""
        return a + b

    assert add.name == "add"
    assert add.description == "两数相加。"
    assert add.args_schema["properties"]["a"] == {"type": "integer"}
    assert add.args_schema["required"] == ["a"]
    assert add._run(a=2, b=3) == 5


def test_tool_optional_annotation() -> None:
    @tool
    def greet(name: str, greeting: str | None = None) -> str:
        return f"{greeting or '你好'},{name}"

    props = greet.args_schema["properties"]
    assert props["greeting"]["type"] == "string"
    assert "greeting" not in greet.args_schema["required"]


def test_tool_to_schema_and_spec() -> None:
    @tool
    def calc(x: int) -> int:
        """计算 x 的平方。"""
        return x * x

    schema = calc.to_schema()
    assert schema["function"]["name"] == "calc"
    assert schema["function"]["parameters"]["properties"]["x"] == {"type": "integer"}

    spec = calc.to_spec()
    assert spec.name == "calc"
    assert spec.fn(x=3) == 9  # prebuilt ToolSpec 可直接调用


def test_tool_invoke_with_tool_call() -> None:
    @tool
    def double(x: int) -> int:
        return x * 2

    out = double.invoke({"tool_call": {"id": "c1", "arguments": {"x": 4}}})
    assert out == {"tool_result": 8, "tool_call_id": "c1"}


def test_tool_invoke_with_proxy_arguments() -> None:
    """host 模式任务输入是 TrackedStateProxy（非 dict）——工具调用应兼容。

    deerflow 移植暴露：BaseTool.invoke 用 isinstance(args, dict) 判断，
    代理对象（`s["tool_call"]` 是 TrackedStateProxy）被误判为标量而包成
    `{"value": args}`，导致 `calculator() got an unexpected keyword argument 'value'`。
    """
    from reactivegraph.state import TrackedStateProxy

    @tool
    def double(x: int) -> int:
        return x * 2

    proxy = TrackedStateProxy({"tool_call": {"id": "c1", "arguments": {"x": 4}}})
    out = double.invoke(proxy)
    assert out == {"tool_result": 8, "tool_call_id": "c1"}


def test_tool_invoke_missing_input_hint() -> None:
    @tool
    def f(x: int) -> int:
        return x

    with pytest.raises(ReactiveChainError, match="tool_call"):
        f.invoke({})


def test_tool_error_captured() -> None:
    @tool
    def boom(x: int) -> int:
        raise ValueError("炸了")

    out = boom.invoke({"tool_call": {"id": "c1", "arguments": {"x": 1}}})
    assert "Error: ValueError: 炸了" in out["tool_result"]


def test_structured_tool_explicit_schema() -> None:
    st = StructuredTool(
        lambda q: f"answer:{q}",
        name="ask",
        description="提问",
        args_schema={"type": "object", "properties": {"q": {"type": "string"}}, "required": ["q"]},
    )
    assert st.args_schema["properties"]["q"]["type"] == "string"
    assert st._run(q="hi") == "answer:hi"


def test_toolkit_dedup_and_error() -> None:
    @tool
    def a() -> str:
        return "a"

    tk = Toolkit([a]).add(a)
    assert len(tk) == 1  # 同实例幂等
    with pytest.raises(ReactiveChainError, match="工具名重复"):
        tk.add(StructuredTool(lambda: 1, name="a", description="dup"))


def test_toolkit_export() -> None:
    @tool
    def x() -> int:
        return 1

    tk = Toolkit([x])
    assert len(tk.to_schemas()) == 1
    assert len(tk.to_specs()) == 1


def test_calculator_tool() -> None:
    assert calculator._run(expression="1 + 2 * 3") == "7"
    assert calculator._run(expression="2 ** 10") == "1024"
    assert calculator._run(expression="pi") == "3.141592653589793"


def test_calculator_rejects_code_injection() -> None:
    # _run 直接抛 ValueError；经 invoke（agent 路径）错误被捕获回传
    with pytest.raises(ValueError):
        calculator._run(expression="__import__('os').system('echo pwned')")
    out = calculator.invoke(
        {
            "tool_call": {
                "id": "c1",
                "arguments": {"expression": "__import__('os').system('echo pwned')"},
            }
        }
    )
    assert "Error" in out["tool_result"] or "无法计算" in out["tool_result"]


def test_builtin_datetime_tools() -> None:
    assert current_date._run().count("-") == 2
    assert len(current_time._run()) == 8


def test_tool_node_adapter_integrates_prebuilt() -> None:
    @tool
    def greet(name: str) -> str:
        """打招呼。"""
        return f"你好，{name}"

    node = ToolNodeAdapter([greet]).node()
    from reactivegraph.prebuilt import ToolNode

    assert isinstance(node, ToolNode)
    results = node([{"id": "c1", "name": "greet", "arguments": {"name": "张三"}}])
    assert results[0]["content"] == "你好，张三"
    assert results[0]["tool_call_id"] == "c1"
    # specs 与 bind_tools schema 同构
    assert node.specs()[0]["function"]["name"] == "greet"


def test_tool_node_adapter_parallel_preserves_order_and_isolates_errors() -> None:
    barrier = threading.Barrier(2)

    @tool
    def wait_and_echo(value: str) -> str:
        """Wait until both parallel calls are running, then echo the value."""
        barrier.wait(timeout=2)
        return f"echo:{value}"

    node = ToolNodeAdapter([wait_and_echo]).node()
    messages = node(
        [
            {"id": "c1", "name": "wait_and_echo", "arguments": {"value": "first"}},
            {"id": "c2", "name": "missing", "arguments": {}},
            {"id": "c3", "name": "wait_and_echo", "arguments": {"value": "third"}},
        ],
        concurrency=2,
    )

    assert [message["tool_call_id"] for message in messages] == ["c1", "c2", "c3"]
    assert messages[0]["content"] == "echo:first"
    assert messages[1]["content"] == "Error: unknown tool: 'missing'"
    assert messages[2]["content"] == "echo:third"


def test_tool_node_adapter_rejects_invalid_concurrency() -> None:
    node = ToolNodeAdapter([]).node()

    with pytest.raises(ValueError, match="concurrency"):
        node([], concurrency=0)


def test_calculator_rejects_power_explosion() -> None:
    """审查反馈：幂爆炸 DoS 防护（指数上限 + 结果位长上限）。"""
    with pytest.raises(ValueError, match="指数过大"):
        calculator._run(expression="9 ** 9 ** 9")
    with pytest.raises(ValueError, match="结果过大"):
        calculator._run(expression="2 ** 200000")  # 约 6 万位 → 超过 4096 位上限
    # 正常幂运算不受影响
    assert calculator._run(expression="2 ** 10") == "1024"


def test_tool_schema_literal_enum() -> None:
    """行业规范（JSON Schema / OpenAI function calling）：Literal → enum
    列表、str Enum 子类 → enum 值列表（不再兜底 string）。"""

    @tool
    def paint(color: _Color, mode: Literal["fast", "slow"]) -> str:
        """上色。"""
        return f"{color.value}:{mode}"

    schema = paint.to_schema()["function"]["parameters"]
    color_schema = schema["properties"]["color"]
    assert color_schema["type"] == "string"
    assert color_schema["enum"] == ["red", "blue"]
    mode_schema = schema["properties"]["mode"]
    assert mode_schema["type"] == "string"
    assert mode_schema["enum"] == ["fast", "slow"]


def test_tool_schema_nested_dict_expanded() -> None:
    """行业规范：嵌套 dict 展开——TypedDict → 递归 properties、
    dict[str, X] → additionalProperties。"""

    @tool
    def search(filters: _Filter, tags: dict[str, int]) -> str:
        """搜索。"""
        return "ok"

    schema = search.to_schema()["function"]["parameters"]
    filters = schema["properties"]["filters"]
    assert filters["type"] == "object"
    assert set(filters["properties"]) == {"age", "name"}
    assert filters["properties"]["age"]["type"] == "integer"
    tags = schema["properties"]["tags"]
    assert tags["type"] == "object"
    assert tags["additionalProperties"]["type"] == "integer"


def test_tool_result_and_reactive_metadata() -> None:
    from reactivechain.tools import ToolResult

    @tool(
        reads={"user.id"},
        writes={"search.results"},
        kind="pure",
        return_direct=True,
    )
    def search(query: str) -> ToolResult:
        """Search documents."""
        return ToolResult(
            content="found 1",
            artifact={"id": "doc-1"},
            patches=[{"path": ["search", "results"], "operation": "set", "value": ["doc-1"]}],
        )

    assert search.reads == {"user.id"}
    assert search.writes == {"search.results"}
    assert search.kind == "pure"
    assert search.return_direct is True
    out = search.invoke({"tool_call": {"id": "c1", "arguments": {"query": "rgp"}}})
    assert out["tool_result"] == "found 1"
    assert out["tool_artifact"] == {"id": "doc-1"}
    assert out["search"] == {"results": ["doc-1"]}
    assert out["tool_call_id"] == "c1"


def test_injected_state_and_tool_call_id() -> None:
    @tool
    def lookup(query: str, state: InjectedState, call_id: InjectedToolCallId) -> str:
        """Lookup a value."""
        return f"{query}:{state['user']['id']}:{call_id}"

    schema = lookup.to_schema()
    assert set(schema["function"]["parameters"]["properties"]) == {"query"}
    assert schema["function"]["parameters"]["required"] == ["query"]
    out = lookup.invoke(
        {
            "user": {"id": "u1"},
            "tool_call": {"id": "c1", "arguments": {"query": "q"}},
        }
    )
    assert out["tool_result"] == "q:u1:c1"


def test_tool_error_policy_and_async_execution() -> None:
    @tool(on_error="raise")
    def boom() -> None:
        """Boom."""
        raise ValueError("bad")

    with pytest.raises(ValueError, match="bad"):
        boom.invoke({"tool_call": {"id": "c1", "arguments": {}}})

    @tool
    def echo(value: str) -> str:
        """Echo."""
        return value

    async def run() -> str:
        result = await echo.ainvoke({"tool_call": {"id": "c2", "arguments": {"value": "ok"}}})
        return str(result["tool_result"])

    import asyncio

    assert asyncio.run(run()) == "ok"


def test_terminal_requires_explicit_policy() -> None:
    from reactivechain.tools import TerminalPolicy, enable_terminal

    with pytest.raises(ReactiveChainError, match="terminal tool disabled"):
        terminal._run(command="echo nope")

    enable_terminal(TerminalPolicy(allowed_commands=("echo",), timeout=1))
    try:
        assert terminal._run(command="echo safe") == "safe"
    finally:
        enable_terminal(None)


def test_public_tool_protocol_exports() -> None:
    import reactivechain

    assert reactivechain.InjectedState == "InjectedState"
    assert reactivechain.InjectedToolCallId == "InjectedToolCallId"
    assert reactivechain.ToolResult is ToolResult
    assert callable(reactivechain.enable_terminal)


def test_injected_secret_and_pydantic_schema() -> None:
    import os

    @tool
    def create(item: _PydanticItem, secret: InjectedSecret) -> str:
        """Create an item."""
        return f"{item.name}:{item.price}:{secret}"

    os.environ["TOOL_TOKEN"] = "tok"
    schema = create.to_schema()
    assert set(schema["function"]["parameters"]["properties"]) == {"item"}
    out = create.invoke(
        {"tool_call": {"id": "c1", "arguments": {"item": {"name": "book", "price": 1.5}}}}
    )
    assert out["tool_result"] == "book:1.5:tok"


def test_tool_streaming_and_tool_node_graph() -> None:
    from reactivechain.tools import ToolStreamChunk

    @tool
    def work(value: str):
        """Stream work."""
        yield ToolStreamChunk({"progress": 0.5})
        yield ToolStreamChunk({"progress": 1.0})
        return ToolResult(content=f"done:{value}", artifact={"value": value})

    chunks = list(work.stream({"tool_call": {"id": "c1", "arguments": {"value": "x"}}}))
    assert chunks[:-1] == [
        {"chunk": {"progress": 0.5}},
        {"chunk": {"progress": 1.0}},
    ]
    assert chunks[-1]["tool_result"] == "done:x"

    node = ToolNodeAdapter(Toolkit([work])).node()
    messages = node([{"id": "c1", "name": "work", "arguments": {"value": "x"}}])
    assert messages[0]["content"] == "done:x"
    assert messages[0]["artifact"] == {"value": "x"}


def test_optional_langchain_adapter_contract() -> None:
    from reactivechain.tools_ext.langchain import from_langchain_tool

    class FakeLangChainTool:
        name = "fake"
        description = "fake tool"
        response_format = "content_and_artifact"
        return_direct = True
        args_schema = {"type": "object", "properties": {}, "required": []}

        def invoke(self, args):
            return "summary", {"artifact": 1}

    adapted = from_langchain_tool(FakeLangChainTool())
    out = adapted.invoke({"tool_call": {"id": "c1", "arguments": {}}})
    assert out["tool_result"] == "summary"
    assert out["tool_artifact"] == {"artifact": 1}
    assert out["tool_return_direct"] is True
    assert adapted.kind == "opaque"


def test_parameterized_injected_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    @tool
    def transfer(amount: float, token: InjectedSecretArg("PAYMENT_TOKEN")) -> str:
        """Transfer money."""
        return f"{amount}:{token}"

    monkeypatch.setenv("PAYMENT_TOKEN", "pay-secret")
    schema = transfer.to_schema()
    assert set(schema["function"]["parameters"]["properties"]) == {"amount"}
    out = transfer.invoke({"tool_call": {"id": "c1", "arguments": {"amount": 2.5}}})
    assert out["tool_result"] == "2.5:pay-secret"


def test_tool_spec_reactive_metadata() -> None:
    @tool(reads={"user.id"}, writes={"search.results"}, kind="pure", receipt=False)
    def lookup(query: str) -> str:
        """Lookup."""
        return query

    spec = lookup.to_spec()
    assert spec.kind == "pure"
    assert spec.reads == ("user.id",)
    assert spec.writes == ("search.results",)
    assert spec.receipt is False


def test_toolkit_to_graph_and_effect_receipt() -> None:
    calls: list[str] = []

    @tool(reads={"input"}, writes={"output"}, kind="effect", receipt=True)
    def charge(amount: int) -> ToolResult:
        """Charge money."""
        calls.append(f"{amount}")
        return ToolResult(
            content=f"charged:{amount}",
            patches=[{"path": ["output"], "operation": "set", "value": amount + 1}],
            artifact={"receipt": f"receipt:{amount}"},
        )

    graph = Toolkit([charge]).to_graph(graph_id="tool_graph")
    out = graph.invoke("run", {"tool_call": {"arguments": {"amount": 1}}})
    assert out["output"] == 2
    # Task result must expose the receipt to ReactiveGraph's effect gate.
    task = graph.definition._task_by_id["charge"]
    assert task is not None
    raw = task.fn({"tool_call": {"arguments": {"amount": 1}}})
    update, receipt = graph._split_update_and_receipt(raw)
    assert update == {"output": 2}
    assert receipt == "receipt:1"


@pytest.fixture
def durable_tool_host(tmp_path: Path):
    node = shutil.which("node")
    dist = REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    if node is None or not dist.exists():
        pytest.skip("bundled Driver (node + packages/driver/dist) not available")
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(dist)
    env["REACTIVEGRAPH_NODE_BIN"] = node
    env["REACTIVEGRAPH_DB"] = str(tmp_path / "tools.db")
    host = DriverHost(env=env)
    host.start()
    host.handshake()
    try:
        yield host
    finally:
        host.close()


def test_tool_effect_receipt_skips_repeat_run(durable_tool_host: DriverHost) -> None:
    calls: list[int] = []

    @tool(reads={"amount"}, writes={"output"}, kind="effect", receipt=True)
    def charge(amount: int) -> ToolResult:
        """Charge money."""
        calls.append(amount)
        return ToolResult(
            content=f"charged:{amount}",
            patches=[{"path": ["output"], "operation": "set", "value": amount + 1}],
            artifact={"receipt": "receipt:1"},
        )

    graph = Toolkit([charge]).to_graph(host=durable_tool_host, graph_id="tool_receipt")
    out1 = graph.invoke("run", {"tool_call": {"arguments": {"amount": 1}}})
    out2 = graph.invoke("run", {"tool_call": {"arguments": {"amount": 1}}})
    assert out1["output"] == 2
    assert out2["output"] == 2
    assert calls == [1]


def test_tool_effect_receipt_survives_driver_restart(tmp_path: Path) -> None:
    node = shutil.which("node")
    dist = REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    if node is None or not dist.exists():
        pytest.skip("bundled Driver (node + packages/driver/dist) not available")
    db = tmp_path / "restart-tools.db"
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(dist)
    env["REACTIVEGRAPH_NODE_BIN"] = node
    env["REACTIVEGRAPH_DB"] = str(db)
    calls: list[int] = []

    @tool(reads={"amount"}, writes={"output"}, kind="effect", receipt=True)
    def charge(amount: int) -> ToolResult:
        """Charge money."""
        calls.append(amount)
        return ToolResult(
            content=f"charged:{amount}",
            patches=[{"path": ["output"], "operation": "set", "value": amount + 1}],
            artifact={"receipt": "receipt:1"},
        )

    host1 = DriverHost(env=env)
    host1.start()
    host1.handshake()
    try:
        graph1 = Toolkit([charge]).to_graph(host=host1, graph_id="tool_receipt_restart")
        assert graph1.invoke("run", {"tool_call": {"arguments": {"amount": 1}}})["output"] == 2
    finally:
        host1.close()

    host2 = DriverHost(env=env)
    host2.start()
    host2.handshake()
    try:
        graph2 = Toolkit([charge]).to_graph(host=host2, graph_id="tool_receipt_restart")
        out = graph2.invoke("run", {"tool_call": {"arguments": {"amount": 1}}})
        assert out["output"] == 2
    finally:
        host2.close()
    assert calls == [1]


def test_tool_streaming_through_driver(durable_tool_host: DriverHost) -> None:
    calls: list[str] = []

    @tool(reads={"amount"}, writes={"output"}, kind="effect")
    def work(amount: int):
        """Stream work."""
        calls.append("start")
        yield ToolStreamChunk({"progress": 0.5})
        yield ToolStreamChunk({"progress": 1.0})
        return ToolResult(
            content=f"done:{amount}",
            patches=[{"path": ["output"], "operation": "set", "value": amount + 1}],
        )

    graph = Toolkit([work]).to_graph(host=durable_tool_host, graph_id="tool_stream")
    events = list(graph.stream("run", {"tool_call": {"arguments": {"amount": 1}}}))
    custom = [e for e in events if e.get("eventType") == "custom"]
    assert [e["payload"]["payload"] for e in custom] == [
        {"progress": 0.5},
        {"progress": 1.0},
    ]
    values = [e for e in events if e.get("eventType") == "values"]
    assert values[-1]["payload"]["state"]["output"] == 2
    assert calls == ["start"]


def test_tool_receipt_skip_explanation(durable_tool_host: DriverHost) -> None:
    calls: list[int] = []

    @tool(reads={"amount"}, writes={"output"}, kind="effect", receipt=True)
    def charge(amount: int) -> ToolResult:
        """Charge money."""
        calls.append(amount)
        return ToolResult(
            content=f"charged:{amount}",
            patches=[{"path": ["output"], "operation": "set", "value": amount + 1}],
            artifact={"receipt": "receipt:1"},
        )

    graph = Toolkit([charge]).to_graph(host=durable_tool_host, graph_id="tool_receipt_explain")
    list(graph.stream("run", {"tool_call": {"arguments": {"amount": 1}}}, trace=True))
    list(graph.stream("run", {"tool_call": {"arguments": {"amount": 1}}}, trace=True))
    explanation = graph.explain_run()
    assert explanation["skipped"] == 1
    decision = graph.why_skipped("charge")
    assert decision == {
        "task": "charge",
        "kind": "effect",
        "reason": "receipt_present",
    }
    assert graph.cost_saved()["skipped"] == 1
    assert calls == [1]


def test_toolkit_graph_parallel_tool_calls_execute_independently(
    durable_tool_host: DriverHost,
) -> None:
    import time

    calls: list[str] = []

    @tool(reads={"x"}, writes={"y"}, kind="effect")
    def slow_a(x: int) -> ToolResult:
        """Slow tool."""
        time.sleep(0.03)
        calls.append("a")
        return ToolResult(content="a", patches=[{"path": ["y"], "operation": "set", "value": x}])

    @tool(reads={"x"}, writes={"z"}, kind="effect")
    def fast_b(x: int) -> ToolResult:
        """Fast tool."""
        calls.append("b")
        return ToolResult(content="b", patches=[{"path": ["z"], "operation": "set", "value": x}])

    graph = Toolkit([slow_a, fast_b]).to_graph(
        host=durable_tool_host, graph_id="tool_parallel_graph"
    )
    graph.invoke("run", {"tool_call": {"arguments": {"x": 1}}})
    assert graph.get_state()["y"] == 1
    assert graph.get_state()["z"] == 1
    assert calls == ["a", "b"]


def test_langchain_message_adapter_normalizes_common_roles() -> None:
    from reactivechain.tools_ext.langchain import from_langchain_message

    class FakeHuman:
        content = "hello"
        type = "human"

    class FakeAIMessage:
        content = "hi"
        type = "ai"
        tool_calls = [{"id": "c1", "name": "tool", "args": {"x": 1}}]

    class FakeToolMessage:
        content = "result"
        type = "tool"
        tool_call_id = "c1"

    messages = [
        from_langchain_message(FakeHuman()),
        from_langchain_message(FakeAIMessage()),
        from_langchain_message(FakeToolMessage()),
    ]
    assert [type(m).__name__ for m in messages] == [
        "HumanMessage",
        "AIMessage",
        "ToolMessage",
    ]
    assert messages[1].tool_calls == [{"id": "c1", "name": "tool", "arguments": {"x": 1}}]
    assert messages[2].tool_call_id == "c1"
