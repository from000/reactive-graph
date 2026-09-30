"""工具层（对应 langchain.tools / langchain_core.tools）。

- `BaseTool` / `@tool` 装饰器：函数 → 工具（args schema 从类型注解推导，
  docstring 作描述；与 prebuilt `ToolSpec` 同构）；
- `StructuredTool`：显式 args schema；
- `Toolkit`：工具集组合；
- `ToolNodeAdapter`：对接 `reactivegraph.prebuilt.ToolNode`（driver 侧执行体，
  reactivechain 只做声明与 schema）。
- 内置纯实现工具：Calculator / Time / Date / Terminal / WebSearch（占位 adapter）。
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import datetime as _dt
import inspect
import json
import os
import shlex
import subprocess
import typing
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, get_type_hints

from typing_extensions import is_typeddict

from .callback import get_callback_manager
from .runnable import ReactiveChainError, Runnable, State

_PY_TO_JSON = {
    int: "integer",
    float: "number",
    bool: "boolean",
    str: "string",
    list: "array",
    dict: "object",
    type(None): "null",
}


def _type_to_schema(t: Any) -> dict[str, Any]:
    """类型注解 → JSON Schema（行业规范：OpenAI function calling / JSON Schema）。

    支持：Optional/Union 折叠、list → array、dict[str, X] → object +
    additionalProperties、Literal → enum、Enum 子类 → enum（成员值）、
    TypedDict → 递归 properties。无法解析的类型保守兜底为 string。
    """
    origin = typing.get_origin(t)
    if origin in (typing.Optional, typing.Union):
        args = [a for a in typing.get_args(t) if a is not type(None)]
        if len(args) == 1:
            return _type_to_schema(args[0])
        return {"anyOf": [_type_to_schema(a) for a in args]}
    if origin is list:
        return {"type": "array", "items": _type_to_schema(typing.get_args(t)[0])}
    if origin is dict:
        dargs = typing.get_args(t)
        schema: dict[str, Any] = {"type": "object"}
        if len(dargs) == 2:
            schema["additionalProperties"] = _type_to_schema(dargs[1])
        return schema
    if origin is typing.Literal:
        values = list(typing.get_args(t))
        sample = values[0] if values else ""
        jtype = _PY_TO_JSON.get(type(sample), "string")
        return {"type": jtype, "enum": values}
    if isinstance(t, type) and issubclass(t, Enum):
        values = [m.value for m in t]
        jtype = "string"
        if values and all(isinstance(v, (str, int, float, bool)) for v in values):
            jtype = _PY_TO_JSON[type(values[0])]
        return {"type": jtype, "enum": values}
    if t in _PY_TO_JSON:
        return {"type": _PY_TO_JSON[t]}
    if isinstance(t, type) and issubclass(t, (str, int, float, bool)):
        return {"type": _PY_TO_JSON[t]}
    if is_typeddict(t):
        hints = get_type_hints(t)
        return {
            "type": "object",
            "properties": {k: _type_to_schema(v) for k, v in hints.items()},
            "required": sorted(hints),
        }
    return {"type": "string"}  # 兜底


def _parse_arguments(raw: Any) -> dict[str, Any]:
    """`tool_calls.arguments` 统一解析（对齐 OpenAI 协议：JSON 字符串形态）。

    dict 原样；合法 JSON 字符串 → json.loads（须为对象）；其余兜底
    `{"value": raw}`（非法 JSON/标量）。工具节点与 agent 循环共用，
    避免两处解析行为分叉。
    """
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {"value": raw}
        if isinstance(parsed, dict):
            return parsed
        return {"value": raw}
    return {"value": raw}


InjectedState = "InjectedState"
InjectedToolCallId = "InjectedToolCallId"
InjectedSecret = "InjectedSecret"


@dataclass(frozen=True)
class InjectedSecretArg:
    """Declare a runtime-injected secret from a named environment key."""

    env_key: str


def _is_injected(hint: Any, marker: str) -> bool:
    return isinstance(hint, typing.ForwardRef) and hint.__forward_arg__ == marker


def _is_injected_secret(hint: Any) -> bool:
    return _is_injected(hint, "InjectedSecret") or isinstance(hint, InjectedSecretArg)


def _injected_secret_env_key(hint: Any) -> str | None:
    if isinstance(hint, InjectedSecretArg):
        return hint.env_key
    if _is_injected(hint, "InjectedSecret"):
        return "TOOL_TOKEN"
    return None


def _safe_type_hints(fn: Callable) -> dict[str, Any]:
    """``get_type_hints`` that tolerates non-type annotation values.

    ``from __future__ import annotations`` stringifies every annotation, and
    ``get_type_hints`` re-evaluates them through ``typing``, which rejects
    values that are not types. A parameterised injected marker such as
    ``token: InjectedSecretArg("PAYMENT_TOKEN")`` evaluates to an *instance*,
    so the whole-call form raises on Python < 3.12 ("Forward references must
    evaluate to types"). Resolve annotations one by one instead: a failed
    entry keeps its raw form rather than poisoning every other parameter.
    """
    raw = getattr(fn, "__annotations__", None) or {}
    globalns = getattr(fn, "__globals__", {})
    hints: dict[str, Any] = {}
    for name, annotation in raw.items():
        if isinstance(annotation, str):
            try:
                evaluated = eval(annotation, globalns)  # noqa: S307 - own module globals
            except Exception:  # pragma: no cover - unresolvable forward refs
                evaluated = annotation
            # ``typing`` wraps a string result in a ForwardRef; the marker
            # checks rely on that, so mirror the behaviour here.
            annotation = typing.ForwardRef(evaluated) if isinstance(evaluated, str) else evaluated
        hints[name] = annotation
    return hints


def _schema_from_fn(fn: Callable) -> dict[str, Any]:
    hints = _safe_type_hints(fn)
    sig = inspect.signature(fn)
    props: dict[str, dict[str, Any]] = {}
    required: list[str] = []
    for name, param in sig.parameters.items():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            continue
        hint = hints.get(name, str)
        injected = (
            _is_injected(hint, "InjectedState")
            or _is_injected(hint, "InjectedToolCallId")
            or _is_injected_secret(hint)
        )
        if injected:
            continue
        props[name] = _type_to_schema(hints.get(name, str))
        if param.default is inspect.Parameter.empty:
            required.append(name)
    return {"type": "object", "properties": props, "required": required}


@dataclass
class ToolResult:
    """Tool execution result for models and program consumers.

    ``content`` is returned to the model, ``artifact`` is for program use, and
    ``patches`` are state updates consumed by ReactiveGraph.
    """

    content: Any
    artifact: Any | None = None
    patches: list[dict[str, Any]] = field(default_factory=list)
    return_direct: bool = False


@dataclass
class ToolStreamChunk:
    """A transient chunk yielded by a streaming tool."""

    chunk: Any


def _apply_patches(output: dict[str, Any], patches: list[dict[str, Any]]) -> None:
    """Apply tool patches in declaration order.

    The reactive engine owns nested path semantics; the chain fallback uses
    dotted top-level paths only. This keeps ToolResult useful without duplicating
    the engine's patch parser.
    """
    for patch in patches:
        path = patch.get("path")
        if not isinstance(path, list) or not path or not all(isinstance(x, str) for x in path):
            raise ReactiveChainError("ToolResult.patches.path must be a non-empty list[str]")
        value = patch.get("value")
        for key in path[:-1]:
            current = output.setdefault(key, {})
            if not isinstance(current, dict):
                raise ReactiveChainError(f"ToolResult patch path {path!r} crosses a non-object")
            output = current
        output[path[-1]] = value


def _validate_args(hints: dict[str, Any], args: dict[str, Any]) -> dict[str, Any]:
    """Validate/convert annotated values before dispatch (currently Pydantic)."""
    validated = dict(args)
    for name, hint in hints.items():
        value = validated.get(name)
        validator = getattr(hint, "model_validate", None)
        if callable(validator) and value is not None and not isinstance(value, hint):
            validated[name] = validator(value)
    return validated


class BaseTool(Runnable):
    """工具基类：子类实现 `_run(**kwargs)`；`name`/`description` 类属性。"""

    name: str = "tool"
    description: str = ""
    args_schema: dict[str, Any] | None = None
    on_error: Literal["return", "raise"] = "return"
    return_direct: bool = False
    kind: str = "effect"
    receipt: bool = False

    @property
    def id(self) -> str:
        return f"tool:{self.name}"

    async def _arun(self, **kwargs: Any) -> Any:
        """默认异步实现：在线程中执行同步工具。"""
        return await asyncio.to_thread(self._run, **kwargs)

    def stream(self, state: State, *, mode: str = "values") -> Iterator[State]:
        """Yield tool chunks, then the final normalized tool output."""
        if mode not in ("values", "messages"):
            raise ReactiveChainError(f"unknown tool stream mode {mode!r}")
        source = getattr(self, "_fn", type(self)._run)
        raw_args = state.get("tool_call", {}).get("arguments", {})
        result = source(**_parse_arguments(raw_args))
        if isinstance(result, Iterator):
            final: Any = None
            while True:
                try:
                    item = next(result)
                except StopIteration as stop:
                    final = stop.value
                    break
                if isinstance(item, ToolStreamChunk):
                    yield {"chunk": item.chunk}
                else:
                    yield {"chunk": item}
            yield self._result_output(state, final)
        else:
            yield self._result_output(state, result)

    def to_spec(self) -> Any:
        """转 prebuilt.ToolSpec（供 ToolNode 执行）。"""
        from reactivegraph.prebuilt import ToolSpec

        def dispatch(**kwargs: Any) -> Any:
            state = {"tool_call": {"id": kwargs.pop("__call_id", ""), "arguments": kwargs}}
            output = self.invoke(state)
            if "tool_artifact" in output:
                return output["tool_result"], output["tool_artifact"]
            return output["tool_result"]

        return ToolSpec(
            name=self.name,
            fn=dispatch,
            description=self.description,
            parameters=self.args_schema,
            kind=self.kind,
            reads=tuple(sorted(self.reads)),
            writes=tuple(sorted(self.writes)),
            receipt=self.receipt,
        )

    def to_schema(self) -> dict[str, Any]:
        """OpenAI tool-call schema（供 bind_tools）。"""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.args_schema or {"type": "object", "properties": {}},
            },
        }

    def _run(self, **kwargs: Any) -> Any:
        raise NotImplementedError  # pragma: no cover

    def _result_output(self, state: State, result: Any) -> State:
        call = state.get("tool_call", {})
        output: dict[str, Any] = {"tool_call_id": call.get("id", "")}
        if isinstance(result, ToolResult):
            output["tool_result"] = result.content
            if result.artifact is not None:
                output["tool_artifact"] = result.artifact
            _apply_patches(output, result.patches)
            output["tool_return_direct"] = result.return_direct or self.return_direct
        else:
            output["tool_result"] = result
        return output

    def invoke(self, state: State) -> State:
        if "tool_call" not in state:
            raise ReactiveChainError(
                f"工具 '{self.name}' 缺少输入键 'tool_call' — Hint: 由 agent 循环提供"
            )
        call = state["tool_call"]
        args = call.get("arguments") or {}
        if not isinstance(args, dict):
            # host 模式任务输入为 TrackedStateProxy（非 dict 子类）——
            # 先转纯 dict，避免被误判为标量而包成 {"value": args}
            snapshot = getattr(args, "snapshot", None)
            if callable(snapshot):
                args = snapshot()
            else:
                args = {"value": args}
        source = getattr(self, "_fn", type(self)._run)
        hints = _safe_type_hints(source)
        call_id = str(call.get("id", ""))
        for name, hint in hints.items():
            if _is_injected(hint, "InjectedState"):
                args.setdefault(name, dict(state))
            elif _is_injected(hint, "InjectedToolCallId"):
                args.setdefault(name, call_id)
            elif (env_key := _injected_secret_env_key(hint)) is not None:
                secret = os.environ.get(env_key)
                if secret is None:
                    raise ReactiveChainError(f"injected secret {env_key} is not configured")
                args.setdefault(name, secret)
        args = _validate_args(hints, args)
        mgr = get_callback_manager()
        mgr.emit("on_tool_start", self, self.name, dict(args))
        try:
            result = self._run(**args)
        except Exception as exc:  # noqa: BLE001 - 工具错误回传 agent
            if self.on_error == "raise":
                raise
            result = f"Error: {type(exc).__name__}: {exc}"
        mgr.emit("on_tool_end", self, self.name, result)
        return self._result_output(state, result)


def tool(
    fn: Callable | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    reads: set[str] | None = None,
    writes: set[str] | None = None,
    kind: str = "effect",
    receipt: bool = False,
    return_direct: bool = False,
    on_error: Literal["return", "raise"] = "return",
):
    """装饰器：把函数包装为 BaseTool（注解推导 schema，docstring 作描述）。"""

    def wrap(f: Callable) -> BaseTool:
        schema = _schema_from_fn(f)
        return _FunctionTool(
            f,
            name=name or f.__name__,
            description=description or inspect.getdoc(f) or "",
            args_schema=schema,
            reads=reads,
            writes=writes,
            kind=kind,
            receipt=receipt,
            return_direct=return_direct,
            on_error=on_error,
        )

    if fn is None:
        return wrap
    return wrap(fn)


class _FunctionTool(BaseTool):
    def __init__(
        self,
        fn: Callable,
        *,
        name: str,
        description: str,
        args_schema: dict[str, Any],
        reads: set[str] | None = None,
        writes: set[str] | None = None,
        kind: str = "effect",
        receipt: bool = False,
        return_direct: bool = False,
        on_error: Literal["return", "raise"] = "return",
    ) -> None:
        self._fn = fn
        self.name = name
        self.description = description
        self.args_schema = args_schema
        self.reads = reads if reads is not None else set()
        self.writes = writes if writes is not None else set()
        self.kind = kind
        self.receipt = receipt
        self.return_direct = return_direct
        self.on_error = on_error

    def _run(self, **kwargs: Any) -> Any:
        result = self._fn(**kwargs)
        if isinstance(result, Iterator):
            final: Any = None
            while True:
                try:
                    next(result)
                except StopIteration as stop:
                    final = stop.value
                    break
            return final
        return result


class StructuredTool(BaseTool):
    """显式 schema 的工具（LangChain StructuredTool 等价）。"""

    def __init__(
        self,
        fn: Callable,
        *,
        name: str,
        description: str,
        args_schema: dict[str, Any] | None = None,
    ) -> None:
        self._fn = fn
        self.name = name
        self.description = description
        self.args_schema = args_schema or _schema_from_fn(fn)

    def _run(self, **kwargs: Any) -> Any:
        return self._fn(**kwargs)


class Toolkit:
    """工具集组合：去重、批量导出 schema/spec。"""

    def __init__(self, tools: Sequence[BaseTool] | None = None) -> None:
        self._tools: dict[str, BaseTool] = {}
        for t in tools or ():
            self.add(t)

    def add(self, t: BaseTool) -> Toolkit:
        existing = self._tools.get(t.name)
        if existing is not None and existing is not t:
            raise ReactiveChainError(f"工具名重复：{t.name} — Hint: 给工具起唯一名")
        self._tools[t.name] = t
        return self

    def __iter__(self):
        return iter(self._tools.values())

    def __len__(self) -> int:
        return len(self._tools)

    def get(self, name: str) -> BaseTool:
        return self._tools[name]

    def to_schemas(self) -> list[dict[str, Any]]:
        return [t.to_schema() for t in self._tools.values()]

    def to_specs(self) -> list[Any]:
        return [t.to_spec() for t in self._tools.values()]

    def to_graph(self, *, graph_id: str | None = None, host: Any = None) -> Any:
        """Compile every native tool into an independent ReactiveGraph task.

        Tool ``kind`` / ``reads`` / ``writes`` become task metadata. A tool
        with ``receipt=True`` returns ``(state_update, receipt)`` so the
        scheduler's effect idempotency gate can prevent duplicate side effects.
        """
        from reactivegraph import ReactiveGraph

        gid = graph_id or f"toolkit_{id(self):x}"

        def build(b: Any) -> None:
            for selected in self:
                b.task(
                    selected.name,
                    kind=selected.kind,
                    fn=_tool_task_fn(selected),
                    on=("run",),
                    reads=tuple(sorted(selected.reads)),
                    writes=tuple(sorted(selected.writes)),
                )

        return ReactiveGraph.build(build, graph_id=gid, host=host)


def _stringify(value: Any) -> Any:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        return str(value)


def _tool_task_fn(selected: BaseTool) -> Callable[[Any], Any]:
    """Adapt a native tool to a ReactiveGraph task result.

    Streaming tools are passed through as sync generators so the DriverHost
    emits each yielded ``ToolStreamChunk`` as a transient custom event before
    committing the final ToolResult.
    """

    def task_fn(state: Any) -> Any:
        snapshot = getattr(state, "snapshot", None)
        plain = snapshot() if callable(snapshot) else dict(state or {})
        raw_call = plain.get("tool_call", {})
        raw_args = raw_call.get("arguments", {}) if isinstance(raw_call, dict) else {}
        args = _parse_arguments(raw_args) if raw_args else plain
        source = getattr(selected, "_fn", type(selected)._run)
        result = source(**args)
        if isinstance(result, Iterator):

            def _streaming():
                final: Any = None
                while True:
                    try:
                        item = next(result)
                    except StopIteration as stop:
                        final = stop.value
                        break
                    if isinstance(item, ToolStreamChunk):
                        yield {"type": "custom", "payload": item.chunk}
                    else:
                        yield item
                output = selected._result_output({"tool_call": {"id": ""}}, final)
                update = _tool_update(output)
                receipt = None
                if selected.receipt:
                    receipt = output.get("tool_artifact", {}).get("receipt")
                patches = [
                    {"path": [key], "operation": "set", "value": value, "taskId": selected.name}
                    for key, value in update.items()
                ]
                return {
                    "reads": list(selected.reads),
                    "patches": patches,
                    "writes": list(update.keys()),
                    "return_value": update,
                    "external_receipts": [{"receipt": receipt}] if receipt is not None else [],
                }

            return _streaming()

        output = selected._result_output({"tool_call": {"id": ""}}, result)
        update = _tool_update(output)
        if selected.receipt:
            receipt = output.get("tool_artifact", {}).get("receipt")
            return update, receipt
        return update

    return task_fn


def _tool_update(output: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in output.items()
        if key not in {"tool_call_id", "tool_result", "tool_artifact", "tool_return_direct"}
    }


class ToolNodeAdapter:
    """对接 prebuilt.ToolNode：reactivechain 工具 → driver 执行体。"""

    def __init__(self, toolkit: Toolkit | Sequence[BaseTool]) -> None:
        self.toolkit = toolkit if isinstance(toolkit, Toolkit) else Toolkit(toolkit)

    def node(self) -> Any:
        from reactivegraph.prebuilt import ToolNode

        class _ReactiveToolNode(ToolNode):
            toolkit = self.toolkit

            def __call__(
                self,
                tool_calls: Iterable[dict[str, Any]],
                *,
                concurrency: int = 1,
            ) -> list[dict[str, Any]]:
                calls = list(tool_calls)
                if concurrency < 1:
                    raise ValueError("ToolNode concurrency must be >= 1")

                def execute(call: dict[str, Any]) -> dict[str, Any]:
                    name = call.get("name") or call.get("function", {}).get("name")
                    try:
                        selected = self.toolkit.get(name or "")
                    except KeyError:
                        return {
                            "role": "tool",
                            "tool_call_id": call.get("id", ""),
                            "name": name or "",
                            "content": f"Error: unknown tool: {name!r}",
                        }
                    raw_args = (
                        call.get("arguments") or call.get("function", {}).get("arguments") or {}
                    )
                    output = selected.invoke(
                        {"tool_call": {"id": call.get("id", ""), "arguments": raw_args}}
                    )
                    message: dict[str, Any] = {
                        "role": "tool",
                        "tool_call_id": call.get("id", ""),
                        "name": name or "",
                        "content": _stringify(output["tool_result"]),
                    }
                    if "tool_artifact" in output:
                        message["artifact"] = output["tool_artifact"]
                    return message

                if concurrency == 1:
                    return [execute(call) for call in calls]
                with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                    return list(pool.map(execute, calls))

        return _ReactiveToolNode(self.toolkit.to_specs())


# -- 内置工具（纯 stdlib 实现） -------------------------------------------


@tool
def calculator(expression: str) -> str:
    """计算算术表达式（安全：仅允许数字/运算符/括号）。"""
    import ast
    import operator

    safe: dict[Any, Any] = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.FloorDiv: operator.floordiv,
        ast.Mod: operator.mod,
        ast.Pow: operator.pow,
        ast.USub: operator.neg,
        ast.UAdd: operator.pos,
    }
    allowed = (ast.Expression, ast.BinOp, ast.UnaryOp, ast.Constant, ast.Name)

    def _eval(node: ast.AST) -> Any:
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.Name) and node.id in ("pi", "e"):
            import math

            return getattr(math, node.id)
        if isinstance(node, ast.BinOp) and type(node.op) in safe:
            left, right = _eval(node.left), _eval(node.right)
            if type(node.op) is ast.Pow:
                # 防幂爆炸 DoS：指数过大（如 9**9**9）直接拒绝。
                if isinstance(right, (int, float)) and abs(right) > 1_000_000:
                    raise ValueError("指数过大（>1e6）")
            return _check_result(safe[type(node.op)](left, right))
        if isinstance(node, ast.UnaryOp) and type(node.op) in safe:
            return _check_result(safe[type(node.op)](_eval(node.operand)))
        raise ValueError(f"不支持的表达式节点：{type(node).__name__}")

    def _check_result(value: Any) -> Any:
        if isinstance(value, int) and value.bit_length() > 4096:
            raise ValueError("结果过大（>4096 位）— Hint: 拆小表达式")
        if isinstance(value, float) and (value != value or abs(value) == float("inf")):
            raise ValueError("结果无效（NaN/Inf）")
        return value

    try:
        tree = ast.parse(expression, mode="eval")
        if not isinstance(tree, allowed):
            raise ValueError("仅允许算术表达式")
        return str(_check_result(_eval(tree.body)))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"无法计算 '{expression}': {exc}") from exc


@tool
def current_time() -> str:
    """返回当前本地时间（HH:MM:SS）。"""
    return _dt.datetime.now().strftime("%H:%M:%S")


@tool
def current_date() -> str:
    """返回今天日期（YYYY-MM-DD）。"""
    return _dt.date.today().isoformat()


@dataclass
class TerminalPolicy:
    """Explicit, least-privilege shell policy for the terminal tool."""

    allowed_commands: tuple[str, ...]
    timeout: int = 10
    max_output_chars: int = 20_000


_terminal_policy: TerminalPolicy | None = None


def enable_terminal(policy: TerminalPolicy | None) -> None:
    """Enable or disable the terminal tool. Disabled is the safe default."""
    global _terminal_policy
    _terminal_policy = policy


@tool
def terminal(command: str) -> str:
    """Run an explicitly allowed shell command with timeout and output limits."""
    if _terminal_policy is None:
        raise ReactiveChainError(
            "terminal tool disabled — Hint: enable_terminal(TerminalPolicy(...))"
        )
    argv = shlex.split(command)
    if not argv or shlex.split(argv[0])[0] not in _terminal_policy.allowed_commands:
        raise ReactiveChainError(f"terminal command not allowed: {argv[0] if argv else ''}")
    try:
        result = subprocess.run(
            argv,
            shell=False,
            capture_output=True,
            text=True,
            timeout=_terminal_policy.timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise ReactiveChainError(f"terminal command timed out: {exc.timeout}s") from exc
    out = (result.stdout or "") + (result.stderr or "")
    return out.strip()[: _terminal_policy.max_output_chars] or "(no output)"


@tool
def web_search(query: str) -> str:
    """网络搜索占位 adapter：接入真实服务前返回占位结果。"""
    return f"[web_search 占位] 未配置搜索服务，查询：{query}"
