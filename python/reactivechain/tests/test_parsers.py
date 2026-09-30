"""M3：输出解析器测试——容错矩阵 + OutputFixing/Retry 包装。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field

import pytest

from reactivechain import ReactiveChainError, RunnableLambda
from reactivechain.llm import FakeLLM
from reactivechain.parsers import (
    CommaSeparatedListOutputParser,
    DatetimeOutputParser,
    EnumOutputParser,
    JsonOutputParser,
    JsonRegexParser,
    OutputFixingParser,
    PydanticOutputParser,
    RetryOutputParser,
    StrOutputParser,
)


def test_str_parser_passthrough() -> None:
    assert StrOutputParser().invoke({"output": " 你好 "}) == {"output": " 你好 "}


def test_str_parser_missing_hint() -> None:
    with pytest.raises(ReactiveChainError, match="缺少输入键 'output'"):
        StrOutputParser().invoke({})


@pytest.mark.parametrize(
    "text,expected",
    [
        ('{"a": 1}', {"a": 1}),
        ('前文 {"x": [1,2]} 后文', {"x": [1, 2]}),
        ("```json\n{\"name\": \"张三\", \"ok\": true}\n```", {"name": "张三", "ok": True}),
        ('[1, "two", 3]', [1, "two", 3]),
        ('{"s": "含}花括号"}', {"s": "含}花括号"}),
    ],
)
def test_json_parser_fault_tolerance(text: str, expected) -> None:
    assert JsonOutputParser().invoke({"output": text}) == {"json_output": expected}


def test_json_parser_failure_hint() -> None:
    with pytest.raises(ReactiveChainError, match="无法从文本中提取 JSON"):
        JsonOutputParser().invoke({"output": "没有任何JSON"})


def test_json_regex_parser_code_block() -> None:
    text = "结果是：\n```json\n{\"v\": 42}\n```\n完毕"
    assert JsonRegexParser().invoke({"output": text}) == {"json_output": {"v": 42}}


def test_comma_list_parser() -> None:
    out = CommaSeparatedListOutputParser().invoke({"output": "苹果, 香蕉、橙子"})
    assert out == {"list_output": ["苹果", "香蕉", "橙子"]}


class _Mood(enum.Enum):
    HAPPY = "开心"
    SAD = "难过"


def test_enum_parser_by_value() -> None:
    assert EnumOutputParser(_Mood).invoke({"output": "开心"}) == {
        "enum_output": _Mood.HAPPY
    }


def test_enum_parser_by_member_name() -> None:
    assert EnumOutputParser(_Mood).invoke({"output": "HAPPY"}) == {
        "enum_output": _Mood.HAPPY
    }


def test_enum_parser_failure_hint() -> None:
    with pytest.raises(ReactiveChainError, match="不是 _Mood 的成员"):
        EnumOutputParser(_Mood).invoke({"output": "愤怒"})


def test_datetime_parser_iso() -> None:
    out = DatetimeOutputParser().invoke({"output": "2026-09-15T08:30:00"})
    assert out["datetime_output"].isoformat() == "2026-09-15T08:30:00"


def test_datetime_parser_z_suffix() -> None:
    out = DatetimeOutputParser().invoke({"output": "2026-09-15T08:30:00Z"})
    assert out["datetime_output"].isoformat() == "2026-09-15T08:30:00"


def test_datetime_parser_failure_hint() -> None:
    with pytest.raises(ReactiveChainError, match="无法解析时间"):
        DatetimeOutputParser().invoke({"output": "昨天"})


@dataclass
class _Person:
    name: str = field(metadata={"description": "姓名"})
    age: int = field(metadata={"description": "年龄"})
    city: str = "未知"


def test_structured_parser_success() -> None:
    parser = PydanticOutputParser(_Person)
    out = parser.invoke({"output": '{"name": "张三", "age": 30}'})
    assert out["structured"] == _Person(name="张三", age=30, city="未知")
    assert "format_instructions" in out


def test_structured_parser_validation_failure_hint() -> None:
    parser = PydanticOutputParser(_Person)
    with pytest.raises(ReactiveChainError, match="缺失|校验|positional"):
        parser.invoke({"output": '{"name": "张三"}'})


def test_structured_parser_not_object_hint() -> None:
    with pytest.raises(ReactiveChainError, match="期望 JSON 对象"):
        PydanticOutputParser(_Person).invoke({"output": "[1,2]"})
    with pytest.raises(ReactiveChainError, match="需要 dataclass"):
        PydanticOutputParser(int)


def test_output_fixing_parser_recovers() -> None:
    bad = JsonOutputParser()
    llm = FakeLLM(['{"fixed": true}'])
    parser = OutputFixingParser(bad, llm)
    out = parser.invoke({"output": "{坏 JSON"})
    assert out == {"json_output": {"fixed": True}}


def test_retry_parser_succeeds_eventually() -> None:
    calls: list[int] = []

    class _Flaky(RunnableLambda):
        pass

    def flaky(state: dict) -> dict:
        calls.append(1)
        if len(calls) < 2:
            raise ReactiveChainError("暂时失败")
        return {"v": 1}

    parser = RetryOutputParser(RunnableLambda(flaky, writes={"v"}))
    assert parser.invoke({"a": 1}) == {"v": 1}
    assert len(calls) == 2


def test_parser_in_pipeline_with_fake_llm() -> None:
    chain = FakeLLM(['{"value": 7}']) | JsonOutputParser()
    out = chain.invoke({"messages": []})
    assert out == {"json_output": {"value": 7}}


def test_retry_with_error_parser_injects_error() -> None:
    """审查反馈：RetryWithError 每次失败把错误注入 state['error']。"""
    from reactivechain import RetryWithErrorOutputParser

    state_log: list[dict] = []

    class _AlwaysFail(RunnableLambda):
        def invoke(self, state):  # type: ignore[override]
            state_log.append(dict(state))
            raise ReactiveChainError("解析错误 X")

    parser = RetryWithErrorOutputParser(
        _AlwaysFail(lambda s: {"v": 1}, writes={"v"}), max_attempts=3
    )
    with pytest.raises(ReactiveChainError, match="解析错误 X"):
        parser.invoke({"output": "x"})
    assert len(state_log) == 3
    assert "error" not in state_log[0]  # 首次无错误注入
    for seen in state_log[1:]:
        assert "解析错误 X" in seen["error"]  # 后续每次注入错误文本