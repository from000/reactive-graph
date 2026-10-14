"""ReactiveChain 教程 03：工具型 agent（tool calling 循环）。

真实场景用 OpenAICompatChatModel；本教程为离线可运行，用继承
BaseLanguageModel 的 mock 模型模拟 tool_calls 协议。

运行：uv run --directory python/reactivechain python docs/tutorials/code/reactchain_03_tool_agent.py
"""
import os
import sys

# 引导：把 reactivechain 源码包加入 sys.path（教程脚本独立可跑）
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "python", "reactivechain"))
)

from reactivechain import (
    ReactiveChainError,
    Toolkit,
    create_tool_calling_agent,
    tool,
)
from reactivechain.llm import BaseLanguageModel


# --- 1. @tool 装饰器：docstring 作描述、类型注解推导 args schema ---
@tool
def calculator(expression: str) -> str:
    """计算算术表达式（仅支持数字与 + - * / **）。"""
    # 安全求值：只允许数字/运算符（教程用 eval 的白名单简化版）
    allowed = set("0123456789+-*/(). ")
    if not set(expression) <= allowed:
        raise ValueError("包含不允许的字符")
    return str(eval(expression))  # noqa: S307 - 教程白名单演示


# --- 2. 模拟支持 tool_calls 的模型（真模型：OpenAICompatChatModel） ---
class MockToolLLM(BaseLanguageModel):
    supports_tool_calling = True

    def bind_tools(self, schemas):
        return None

    def __init__(self) -> None:
        super().__init__(name="mock_tool_llm")
        self._round = 0

    def _call(self, messages: list[dict]) -> dict:
        self._round += 1
        if self._round == 1:
            return {
                "role": "assistant",
                "content": "我来算一下",
                "tool_calls": [
                    {
                        "id": "c1",
                        "type": "function",
                        "function": {"name": "calculator", "arguments": {"expression": "2 + 3 * 4"}},
                    }
                ],
            }
        return {"role": "assistant", "content": "结果是 14"}

    def _stream_deltas(self, messages):  # pragma: no cover
        yield "x"


# --- 3. 构建 agent：模型 + 工具集 + 系统提示 ---
agent = create_tool_calling_agent(
    MockToolLLM(),
    Toolkit([calculator]),
    system_prompt="你是计算助手，用工具完成任务。",
    max_iterations=5,
)

out = agent.invoke({"messages": [{"role": "user", "content": "2+3*4=?"}]})
print("3) agent 回答：", out["output"])
assert out["output"] == "结果是 14"

# --- 4. 递归上限：模型一直请求工具时给出清晰错误 ---
class LoopLLM(BaseLanguageModel):
    supports_tool_calling = True

    def bind_tools(self, schemas):
        return None

    def _call(self, messages: list[dict]) -> dict:
        return {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {"id": "c9", "type": "function", "function": {"name": "calculator", "arguments": {"expression": "1+1"}}}
            ],
        }

    def _stream_deltas(self, messages):  # pragma: no cover
        yield "x"


stuck = create_tool_calling_agent(LoopLLM(), Toolkit([calculator]), max_iterations=3)
try:
    stuck.invoke({"messages": [{"role": "user", "content": "继续"}]})
    raise AssertionError("应当触发上限错误")
except ReactiveChainError as exc:
    print("4) 上限保护：", str(exc)[:60], "…")

print("教程 03 OK")