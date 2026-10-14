"""记忆组件（对应 langchain.memory）。

设计：
- `BaseMemory`：`load()` 注入状态键（如 `history`）、`save(inputs, outputs)` 记录对话；
- thread 维度：`save(..., thread_id=...)` / `load(thread_id=...)`，进程内由
  `MemoryStore` 按 thread 隔离；RGP/1 STORE_OP 持久化集成点为 `persist` 回调
  （图集成时挂到 checkpoint/thread 语义，见设计文档 §4.6）。
- Memory 是 Runnable：管道中把历史注入 `history` 键，供
  ChatPromptTemplate 的 `MessagePlaceholder("history")` 消费。
"""

from __future__ import annotations

import re
import threading
from collections import defaultdict, deque
from collections.abc import Callable, Sequence
from typing import Any

from .llm import BaseLanguageModel
from .messages import AIMessage, BaseMessage, HumanMessage
from .runnable import ReactiveChainError, Runnable, State

DEFAULT_THREAD = "default"


class MemoryStore:
    """进程内线程安全记忆库（thread_id → 历史列表）。"""

    def __init__(self) -> None:
        """Initialise the MemoryStore."""
        self._data: dict[str, list[BaseMessage]] = defaultdict(list)
        self._lock = threading.RLock()

    def get(self, thread_id: str) -> list[BaseMessage]:
        """Read one stored value."""
        with self._lock:
            return list(self._data.get(thread_id, []))

    def set(self, thread_id: str, messages: list[BaseMessage]) -> None:
        with self._lock:
            self._data[thread_id] = list(messages)

    def update(
        self,
        thread_id: str,
        fn: Callable[[list[BaseMessage]], list[BaseMessage]],
    ) -> list[BaseMessage]:
        """原子更新：锁内「读取→变换→写回」，返回写回后的消息副本。

        避免并发 save 的 read-modify-write 跨两次加锁导致丢更新。
        """
        with self._lock:
            current = list(self._data.get(thread_id, []))
            updated = fn(current)
            self._data[thread_id] = list(updated)
            return list(updated)

    def clear(self, thread_id: str | None = None) -> None:
        with self._lock:
            if thread_id is None:
                self._data.clear()
            else:
                self._data.pop(thread_id, None)

    def __len__(self) -> int:
        """Number of live entries."""
        with self._lock:
            return sum(len(v) for v in self._data.values())


class BaseMemory(Runnable):
    """记忆抽象：`load` 注入状态键、`save` 记录对话。"""

    writes: set[str] = {"history"}

    def __init__(
        self,
        *,
        store: MemoryStore | None = None,
        thread_id: str = DEFAULT_THREAD,
        persist: Any | None = None,
        name: str | None = None,
    ) -> None:
        """Initialise the BaseMemory."""
        self.store = store if store is not None else MemoryStore()
        self.thread_id = thread_id
        self.persist = persist  # 可选 STORE_OP 集成回调：persist(thread_id, messages)
        self.name = name or type(self).__name__

    @property
    def id(self) -> str:
        """Deterministic identifier used in cache keys and error messages."""
        return f"memory:{self.name}:{self.thread_id}"

    # -- Runnable ----------------------------------------------------------

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        return self.load(state)

    def load(self, state: State | None = None) -> State:
        """Load and return the backing data."""
        raise NotImplementedError  # pragma: no cover

    def save(self, inputs: State, outputs: State, *, thread_id: str | None = None) -> None:
        """Persist the given items."""
        raise NotImplementedError  # pragma: no cover

    def _messages(self, thread_id: str | None = None) -> list[BaseMessage]:
        return self.store.get(thread_id or self.thread_id)

    def _persist_if_set(self, messages: list[BaseMessage], *, thread_id: str | None = None) -> None:
        if self.persist is not None:
            self.persist(thread_id or self.thread_id, [m.to_api_dict() for m in messages])


class BaseChatMemory(BaseMemory):
    """会话记忆：消息成对（user → ai）保存为时间线。"""

    def save(self, inputs: State, outputs: State, *, thread_id: str | None = None) -> None:
        """Persist the given items."""
        tid = thread_id or self.thread_id

        def _append(msgs: list[BaseMessage]) -> list[BaseMessage]:
            if "input" in inputs or "output" in outputs:
                msgs.append(HumanMessage(_as_text(inputs.get("input", inputs))))
                msgs.append(AIMessage(_as_text(outputs.get("output", outputs))))
            return self._trim(msgs)

        saved = self.store.update(tid, _append)
        self._persist_if_set(saved, thread_id=tid)

    def _trim(self, messages: list[BaseMessage]) -> list[BaseMessage]:
        """Trim the conversation to the configured budget."""
        return messages


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if "content" in value:
            return str(value["content"])
        if "output" in value:
            return str(value["output"])
    return str(value)


class ConversationBufferMemory(BaseChatMemory):
    """全量缓冲：历史全部注入 `history` 键。"""

    def load(self, state: State | None = None) -> State:
        """Load and return the backing data."""
        return {"history": self._messages()}


class ConversationBufferWindowMemory(BaseChatMemory):
    """滑动窗口：只保留最近 k 轮（默认最近 2 轮 = 4 条消息）。"""

    def __init__(self, k: int = 2, **kwargs: Any) -> None:
        """Initialise the ConversationBufferWindowMemory."""
        super().__init__(**kwargs)
        if k < 1:
            raise ReactiveChainError("窗口记忆的 k 必须 ≥ 1 — Hint: k 用轮数")
        self.k = k

    def _trim(self, messages: list[BaseMessage]) -> list[BaseMessage]:
        """Trim the conversation to the configured budget."""
        return messages[-self.k * 2 :]

    def load(self, state: State | None = None) -> State:
        """Load and return the backing data."""
        return {"history": self._messages()}


class ConversationSummaryMemory(BaseMemory):
    """LLM 摘要记忆：历史折叠为一句话摘要注入 `summary` 键。"""

    writes: set[str] = {"summary", "history"}

    def __init__(self, llm: BaseLanguageModel, **kwargs: Any) -> None:
        """Initialise the ConversationSummaryMemory."""
        super().__init__(**kwargs)
        self.llm = llm

    def load(self, state: State | None = None) -> State:
        """Load and return the backing data."""
        return {"summary": _build_summary(self._messages()), "history": self._messages()}

    def save(self, inputs: State, outputs: State, *, thread_id: str | None = None) -> None:
        """Persist the given items."""
        tid = thread_id or self.thread_id
        msgs = self.store.get(tid)
        msgs.append(HumanMessage(_as_text(inputs.get("input", inputs))))
        msgs.append(AIMessage(_as_text(outputs.get("output", outputs))))
        summary = self.llm.invoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": (
                            "用一句话概括这段对话的关键信息："
                            + "\n".join(f"{m.role}: {m.content}" for m in msgs[-6:])
                        ),
                    }
                ]
            }
        )["output"]
        self.store.set(tid, [AIMessage(f"[摘要] {summary}")])
        self._persist_if_set([AIMessage(f"[摘要] {summary}")], thread_id=tid)


def _build_summary(messages: list[BaseMessage]) -> str:
    return "\n".join(f"{m.role}: {m.content}" for m in messages if m.content)


def _approx_tokens(messages: Sequence[BaseMessage]) -> int:
    """近似 token 计数：按 4 字符/token 粗略估算（tiktoken 为可选依赖）。"""
    chars = sum(len(m.content) for m in messages)
    return max(1, chars // 4)


class ConversationTokenBufferMemory(BaseChatMemory):
    """按 token 预算截断：超出时从最早消息丢弃（≈ LangChain TokenBufferMemory）。"""

    def __init__(self, max_tokens: int = 2000, **kwargs: Any) -> None:
        """Initialise the ConversationTokenBufferMemory."""
        super().__init__(**kwargs)
        if max_tokens < 1:
            raise ReactiveChainError("TokenBufferMemory 的 max_tokens 必须 ≥ 1")
        self.max_tokens = max_tokens

    def load(self, state: State | None = None) -> State:
        """Load and return the backing data."""
        return {"history": self._messages()}  # 截断在 save 时已完成

    def _trim(self, messages: list[BaseMessage]) -> list[BaseMessage]:
        """Trim the conversation to the configured budget."""
        if _approx_tokens(messages) <= self.max_tokens:
            return messages
        kept: deque[BaseMessage] = deque()
        budget = self.max_tokens
        for m in reversed(messages):
            cost = _approx_tokens([m])
            if budget - cost < 0 and kept:
                break
            kept.appendleft(m)
            budget -= cost
        return list(kept)


class SummaryBufferMemory(ConversationBufferWindowMemory):
    """摘要 + 窗口混合：窗口内全量，窗口外折叠进 summary。"""

    writes: set[str] = {"summary", "history"}

    def _trim(self, messages: list[BaseMessage]) -> list[BaseMessage]:
        """Trim the conversation to the configured budget."""
        return messages  # 保留全量，load 时再拆分窗口与摘要

    def load(self, state: State | None = None) -> State:
        """Load and return the backing data."""
        msgs = self._messages()
        window = msgs[-self.k * 2 :]
        older = msgs[: -self.k * 2] if len(msgs) > self.k * 2 else []
        return {
            "history": window,
            "summary": _build_summary(older),
        }


_ENTITY_RE = re.compile(r"([\u4e00-\u9fffA-Za-z][\u4e00-\u9fffA-Za-z0-9_.]{1,19})")


class EntityMemory(BaseMemory):
    """实体抽取记忆：从消息中提取专名并持续积累（简化启发式）。"""

    writes: set[str] = {"entities"}

    def __init__(self, *, entity_regex: str | None = None, **kwargs: Any) -> None:
        """Initialise the EntityMemory."""
        super().__init__(**kwargs)
        self._entity_re = re.compile(entity_regex) if entity_regex else _ENTITY_RE

    def load(self, state: State | None = None) -> State:
        """Load and return the backing data."""
        raw = self.store.get(f"{self.thread_id}:entities")
        return {"entities": raw[0].content if raw else ""}

    def save(self, inputs: State, outputs: State, *, thread_id: str | None = None) -> None:
        """Persist the given items."""
        tid = thread_id or self.thread_id
        text = f"{_as_text(inputs.get('input', inputs))} {_as_text(outputs.get('output', outputs))}"
        found = sorted(
            {m.group(0) for m in self._entity_re.finditer(text)}
            - {"input", "output", "content"}
        )
        if not found:
            return
        key = f"{tid}:entities"
        existing = self.store.get(key)
        merged = list(dict.fromkeys([*(m.content for m in existing), *found]))
        self.store.set(key, [AIMessage(",".join(merged))])