"""输出解析器（对应 langchain_core.output_parsers）。

所有解析器都是 Runnable：reads 上游输出键，writes 结构化键。
解析失败抛 `ReactiveChainError`（Hint 风格），可被 OutputFixing/Retry 包装器修复。
"""

from __future__ import annotations

import datetime as _dt
import enum
import json
import re
from dataclasses import fields, is_dataclass
from typing import Any, TypeVar

from .runnable import ReactiveChainError, Runnable, State

T = TypeVar("T")


def _require(state: State, key: str, parser: str) -> str:
    if key not in state:
        raise ReactiveChainError(
            f"{parser} 缺少输入键 '{key}' — Hint: 上游需用模型/前段写入该键"
        )
    return str(state[key])


class StrOutputParser(Runnable):
    """文本直通：取上游 `output`，规范化输出 `{"output": str}`。"""

    reads: set[str] = {"output"}
    writes: set[str] = {"output"}

    def __init__(self, *, name: str | None = None) -> None:
        """Initialise the StrOutputParser."""
        self.name = name or "str_parser"

    @property
    def id(self) -> str:
        """Deterministic identifier used in cache keys and error messages."""
        return f"parser:{self.name}"

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        return {"output": _require(state, "output", self.name)}


def _extract_json(text: str) -> Any:
    """从文本中容错提取第一个 JSON 值（对象或数组）。"""
    stripped = text.strip()
    for opener, closer in (("{", "}"), ("[", "]")):
        start = stripped.find(opener)
        if start == -1:
            continue
        depth = 0
        in_str = False
        esc = False
        for i in range(start, len(stripped)):
            ch = stripped[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == opener:
                depth += 1
            elif ch == closer:
                depth -= 1
                if depth == 0:
                    candidate = stripped[start : i + 1]
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        break
    raise ReactiveChainError(
        f"无法从文本中提取 JSON — Hint: 检查模型输出；原始片段：{stripped[:120]!r}"
    )


class JsonOutputParser(Runnable):
    """容错 JSON 提取（对象/数组），输出 `{"json_output": <parsed>}`。"""

    reads: set[str] = {"output"}
    writes: set[str] = {"json_output"}

    def __init__(self, *, name: str | None = None) -> None:
        """Initialise the JsonOutputParser."""
        self.name = name or "json_parser"

    @property
    def id(self) -> str:
        """Deterministic identifier used in cache keys and error messages."""
        return f"parser:{self.name}"

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        return {"json_output": _extract_json(_require(state, "output", self.name))}


class JsonRegexParser(JsonOutputParser):
    """容错提取 JSON 块后解析（兼容曾用正则定位的调用方）。

    实现复用 `_extract_json`（深度扫描，支持嵌套对象/数组），消除与
    JsonOutputParser 的双实现（正则 `{.*?}` 对嵌套 JSON 会截断）。
    """

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        return {"json_output": _extract_json(_require(state, "output", self.name))}


class CommaSeparatedListOutputParser(Runnable):
    """逗号分隔列表 → `{"list_output": [str, ...]}`。"""

    reads: set[str] = {"output"}
    writes: set[str] = {"list_output"}

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        text = _require(state, "output", "list_parser").strip()
        if not text:
            raise ReactiveChainError("列表解析器收到空文本 — Hint: 检查模型输出")
        items = [p.strip() for p in re.split(r"[,，、]", text) if p.strip()]
        return {"list_output": items}


class EnumOutputParser(Runnable):
    """枚举值解析：把输出映射到 enum 成员，输出 `{"enum_output": <member>}`。"""

    reads: set[str] = {"output"}
    writes: set[str] = {"enum_output"}

    def __init__(self, enum_cls: type[enum.Enum], *, name: str | None = None) -> None:
        """Initialise the EnumOutputParser."""
        self.enum_cls = enum_cls
        self.name = name or f"enum_parser:{enum_cls.__name__}"

    @property
    def id(self) -> str:
        """Deterministic identifier used in cache keys and error messages."""
        return f"parser:{self.name}"

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        raw = _require(state, "output", self.name).strip()
        for member in self.enum_cls:
            if raw == member.value or raw == member.name:
                return {"enum_output": member}
        raise ReactiveChainError(
            f"'{raw}' 不是 {self.enum_cls.__name__} 的成员（可选："
            f"{[m.value for m in self.enum_cls]}）— Hint: 检查模型输出"
        )


class DatetimeOutputParser(Runnable):
    """ISO 时间解析（含常见容错格式），输出 `{"datetime_output": datetime}`。"""

    reads: set[str] = {"output"}
    writes: set[str] = {"datetime_output"}

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        raw = _require(state, "output", "datetime_parser").strip()
        normalized = raw.replace("Z", "+00:00")
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = _dt.datetime.strptime(normalized[:19], fmt)
                return {"datetime_output": parsed}
            except ValueError:
                continue
        try:
            return {"datetime_output": _dt.date.fromisoformat(raw[:10])}
        except ValueError as exc:
            raise ReactiveChainError(
                f"无法解析时间 '{raw}' — Hint: 期望 ISO 格式 YYYY-MM-DD[T]HH:MM:SS"
            ) from exc


def _schema_for(cls: type) -> dict[str, Any]:
    """dataclass → JSON Schema（字段类型复用 `tools._type_to_schema`，
    能力对齐：list/dict/Enum/Optional 等类型注解均可描述，非仅基础类型）。"""
    import typing

    from .tools import _type_to_schema

    hints = typing.get_type_hints(cls)
    props: dict[str, dict[str, Any]] = {}
    for f in fields(cls):
        schema = _type_to_schema(hints.get(f.name, str))
        schema["description"] = f.metadata.get("description", f.name)
        props[f.name] = schema
    return {"type": "object", "properties": props, "required": [f.name for f in fields(cls)]}


class PydanticOutputParser(Runnable):
    """结构化输出：按 dataclass/pydantic 模型校验，输出 `{"structured": <实例>}`。

    提示注入：默认在 reads 之外额外产出 `format_instructions` 供提示模板引用
    （`{format_instructions}`）。pydantic 为可选依赖；dataclass 走 stdlib。
    """

    reads: set[str] = {"output"}
    writes: set[str] = {"structured", "format_instructions"}

    def __init__(
        self,
        schema: type,
        *,
        name: str | None = None,
        inject_instructions: bool = True,
    ) -> None:
        """Initialise the PydanticOutputParser."""
        if not (is_dataclass(schema) or _is_pydantic(schema)):
            raise ReactiveChainError(
                f"PydanticOutputParser 需要 dataclass 或 pydantic 模型，得到 {schema}"
            )
        self.schema = schema
        self.name = name or f"structured_parser:{schema.__name__}"
        self.inject_instructions = inject_instructions
        self._schema_dict = (
            _schema_for(schema) if is_dataclass(schema) else schema.model_json_schema()  # type: ignore[attr-defined]
        )
        self.format_instructions = (
            "以 JSON 输出，结构必须匹配：\n"
            + json.dumps(self._schema_dict, ensure_ascii=False, indent=None)
        )

    @property
    def id(self) -> str:
        """Deterministic identifier used in cache keys and error messages."""
        return f"parser:{self.name}"

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        parsed = _extract_json(_require(state, "output", self.name))
        out = {"structured": self._validate(parsed)}
        if self.inject_instructions:
            out["format_instructions"] = self.format_instructions
        return out

    def _validate(self, data: Any) -> Any:
        if is_dataclass(self.schema):
            if not isinstance(data, dict):
                raise ReactiveChainError(
                    f"结构化解析期望 JSON 对象，得到 {type(data).__name__} — "
                    f"Hint: 提示模型输出对象；schema={json.dumps(self._schema_dict)[:80]}"
                )
            try:
                return self.schema(**data)
            except TypeError as exc:
                raise ReactiveChainError(
                    f"结构化校验失败：{exc} — Hint: 对照 format_instructions 检查字段"
                ) from exc
        return self.schema.model_validate(data)  # type: ignore[attr-defined]  # pydantic 可选


def _is_pydantic(schema: type) -> bool:
    try:
        return hasattr(schema, "model_validate") and hasattr(schema, "model_json_schema")
    except Exception:  # pragma: no cover
        return False


class OutputFixingParser(Runnable):
    """解析失败 → 把错误与原文交给 LLM 修复 → 重解析。"""

    def __init__(
        self,
        base_parser: Runnable,
        llm: Runnable,
        *,
        name: str | None = None,
        max_attempts: int = 2,
    ) -> None:
        """Initialise the OutputFixingParser."""
        self.base_parser = base_parser
        self.llm = llm
        self.max_attempts = max_attempts
        self.name = name or f"output_fixing:{base_parser.id}"

    @property
    def id(self) -> str:
        """Deterministic identifier used in cache keys and error messages."""
        return f"parser:{self.name}"

    @property
    def reads(self) -> set[str]:
        """State keys this segment reads (empty = receive the whole state)."""
        return set(self.base_parser.reads)

    @property
    def writes(self) -> set[str]:
        """State keys this segment writes."""
        return set(self.base_parser.writes)

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        for _ in range(self.max_attempts):
            try:
                return self.base_parser.invoke(state)
            except ReactiveChainError as exc:
                fixed = self.llm.invoke(
                    {
                        "messages": [
                            {
                                "role": "user",
                                "content": (
                                    f"修复以下解析问题并只输出修正后的内容：\n"
                                    f"错误：{exc}\n原文：{state.get('output')}"
                                ),
                            }
                        ]
                    }
                )
                state = dict(state, output=fixed["output"])
        return self.base_parser.invoke(state)


class RetryOutputParser(Runnable):
    """解析失败重试（不修改输入）。"""

    def __init__(
        self,
        base_parser: Runnable,
        *,
        max_attempts: int = 3,
        name: str | None = None,
    ) -> None:
        """Initialise the RetryOutputParser."""
        self.base_parser = base_parser
        self.max_attempts = max_attempts
        self.name = name or f"retry:{base_parser.id}"

    @property
    def id(self) -> str:
        """Deterministic identifier used in cache keys and error messages."""
        return f"parser:{self.name}"

    @property
    def reads(self) -> set[str]:
        """State keys this segment reads (empty = receive the whole state)."""
        return set(self.base_parser.reads)

    @property
    def writes(self) -> set[str]:
        """State keys this segment writes."""
        return set(self.base_parser.writes)

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        last: Exception | None = None
        for _ in range(self.max_attempts):
            try:
                return self.base_parser.invoke(state)
            except ReactiveChainError as exc:
                last = exc
        assert last is not None
        raise last


class RetryWithErrorOutputParser(RetryOutputParser):
    """解析失败携带错误重试（配合提示模板中的 `{error}` 变量）。

    每次失败把错误文本注入 `state["error"]`，供模板 `{error}` 占位符消费；
    若模板未声明 `{error}`，注入无副作用，重试退化为 RetryOutputParser。
    """

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        last: Exception | None = None
        for _ in range(max(1, self.max_attempts)):
            try:
                return self.base_parser.invoke(state)
            except ReactiveChainError as exc:
                last = exc
                state = dict(state, error=str(exc))
        if last is None:  # 防御分支（max_attempts<=0 时仍至少尝试一次）
            raise ReactiveChainError("RetryWithErrorOutputParser 重试次数无效")
        raise last