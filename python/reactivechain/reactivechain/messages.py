"""消息模型：BaseMessage 家族（对应 langchain_core.messages）。

角色与 OpenAI 兼容 API 对齐：system / user / assistant / tool。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BaseMessage:
    """一条对话消息。"""

    content: str
    role: str
    name: str | None = None
    additional_kwargs: dict[str, Any] = field(default_factory=dict)

    def to_api_dict(self) -> dict[str, Any]:
        """OpenAI 兼容 API 载荷（无额外字段时精简）。"""
        out: dict[str, Any] = {"role": self.role, "content": self.content}
        if self.name is not None:
            out["name"] = self.name
        out.update(self.additional_kwargs)
        return out


@dataclass
class HumanMessage(BaseMessage):
    role: str = "user"


@dataclass
class AIMessage(BaseMessage):
    role: str = "assistant"

    def __init__(
        self,
        content: str = "",
        *,
        name: str | None = None,
        tool_calls: list[dict[str, Any]] | None = None,
        additional_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Initialise the AIMessage."""
        super().__init__(content, "assistant", name, additional_kwargs or {})
        self.tool_calls = tool_calls or []
        if self.tool_calls:
            self.additional_kwargs["tool_calls"] = self.tool_calls

    @classmethod
    def from_api_message(cls, msg: dict[str, Any]) -> AIMessage:
        """把 OpenAI 兼容的 assistant 消息转回 AIMessage。"""
        return cls(
            content=str(msg.get("content") or ""),
            tool_calls=msg.get("tool_calls"),
            additional_kwargs={
                k: v for k, v in msg.items() if k not in ("role", "content")
            },
        )


@dataclass
class SystemMessage(BaseMessage):
    role: str = "system"


@dataclass
class ToolMessage(BaseMessage):
    """工具执行结果（tool calling 回传）。"""

    role: str = "tool"
    tool_call_id: str | None = None

    def to_api_dict(self) -> dict[str, Any]:
        out = super().to_api_dict()
        if self.tool_call_id:
            out["tool_call_id"] = self.tool_call_id
        return out


def messages_to_api(messages: list[BaseMessage]) -> list[dict[str, Any]]:
    """消息列表 → OpenAI 兼容载荷。"""
    return [m.to_api_dict() for m in messages]