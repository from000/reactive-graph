"""提示模板（对应 langchain_core.prompts）。

- `PromptTemplate`：{var} 插值，输出 `{"prompt": str}`。
- `ChatPromptTemplate`：消息列表模板，输出 `{"messages": [API dict, ...]}`。
- `MessagePlaceholder`：把状态中的消息列表展开到对话模板。
- `FewShotPromptTemplate` / `PipelinePromptTemplate`：组合模板。

所有模板都是 Runnable：reads 从模板变量自动提取。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .messages import BaseMessage
from .runnable import ReactiveChainError, Runnable, State

_VAR = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _extract_vars(template: str) -> set[str]:
    return set(_VAR.findall(template))


def _format(template: str, values: Mapping[str, Any]) -> str:
    try:
        return template.format(**values)
    except KeyError as exc:
        raise ReactiveChainError(
            f"模板缺少变量 {exc} — Hint: 输入需包含模板声明的全部变量"
        ) from exc


class PromptTemplate(Runnable):
    """字符串模板段：`{var}` 插值，输出写键 `prompt`。"""

    writes: set[str] = {"prompt"}

    def __init__(
        self,
        template: str,
        *,
        name: str | None = None,
        partial_variables: dict[str, Any] | None = None,
    ) -> None:
        self.template = template
        self.name = name or f"prompt:{template[:24]!r}"
        self.partial_variables = dict(partial_variables or {})
        self.reads = _extract_vars(template) - set(self.partial_variables)

    @property
    def id(self) -> str:
        return f"prompt:{self.name}"

    def format(self, state: State) -> str:
        values = dict(self.partial_variables)
        missing = [v for v in self.reads if v not in state]
        if missing:
            raise ReactiveChainError(
                f"模板 '{self.name}' 缺少变量 {missing} — "
                f"Hint: 输入需包含 {sorted(self.reads)}"
            )
        values.update({v: state[v] for v in self.reads})
        return _format(self.template, values)

    def invoke(self, state: State) -> State:
        return {"prompt": self.format(state)}


@dataclass
class MessagePlaceholder:
    """对话模板中的占位符：执行时从状态取出消息列表展开。"""

    variable_name: str

    def messages(self, state: State) -> list[dict[str, Any]]:
        raw = state.get(self.variable_name, [])
        if isinstance(raw, list) and all(isinstance(m, BaseMessage) for m in raw):
            return [m.to_api_dict() for m in raw]
        if isinstance(raw, list) and all(isinstance(m, dict) for m in raw):
            return list(raw)
        raise ReactiveChainError(
            f"MessagePlaceholder '{self.variable_name}' 需要消息列表（BaseMessage "
            f"或 API dict），得到 {type(raw).__name__} — Hint: 上游段需写入该键"
        )


class ChatPromptTemplate(Runnable):
    """对话消息模板。声明方式与 LangChain 一致：

        ChatPromptTemplate([
            ("system", "你是 {lang} 助手"),
            ("human", "{question}"),
            MessagePlaceholder("history"),
        ])
    """

    writes: set[str] = {"messages"}

    def __init__(
        self,
        messages: Sequence[tuple[str, str] | MessagePlaceholder],
        *,
        name: str | None = None,
    ) -> None:
        self.messages = list(messages)
        self.name = name or f"chat_prompt:{id(self):x}"
        self.reads: set[str] = set()
        for item in self.messages:
            if isinstance(item, MessagePlaceholder):
                self.reads.add(item.variable_name)
            else:
                self.reads |= _extract_vars(item[1])

    @property
    def id(self) -> str:
        return f"chat_prompt:{self.name}"

    def to_api_messages(self, state: State) -> list[dict[str, Any]]:
        missing = [v for v in self.reads if v not in state]
        if missing:
            raise ReactiveChainError(
                f"对话模板 '{self.name}' 缺少变量 {missing} — "
                f"Hint: 输入需包含 {sorted(self.reads)}"
            )
        out: list[dict[str, Any]] = []
        role_map = {"human": "user", "ai": "assistant", "system": "system", "tool": "tool"}
        for item in self.messages:
            if isinstance(item, MessagePlaceholder):
                out.extend(item.messages(state))
            else:
                role, template = item
                out.append(
                    {"role": role_map.get(role, role), "content": _format(template, state)}
                )
        return out

    def invoke(self, state: State) -> State:
        return {"messages": self.to_api_messages(state)}


class FewShotPromptTemplate(Runnable):
    """示例注入模板：examples 渲染进 prefix/suffix 之间。"""

    writes: set[str] = {"prompt"}

    def __init__(
        self,
        example_prompt: PromptTemplate,
        examples: Sequence[Mapping[str, Any]],
        template: str,
        *,
        name: str | None = None,
    ) -> None:
        self.example_prompt = example_prompt
        self.examples = list(examples)
        self.template = template
        self.name = name or f"fewshot:{template[:24]!r}"
        self.reads = _extract_vars(template)

    @property
    def id(self) -> str:
        return f"fewshot:{self.name}"

    def invoke(self, state: State) -> State:
        blocks = [
            _format(self.template.replace("{examples}", "%%EXAMPLES%%"), state)
        ]
        rendered = [self.example_prompt.format(dict(ex)) for ex in self.examples]
        joined = "\n".join(rendered)
        text = blocks[0].replace("%%EXAMPLES%%", joined)
        return {"prompt": text}


class PipelinePromptTemplate(Runnable):
    """多模板组合：把若干命名模板渲染进主模板。"""

    writes: set[str] = {"prompt"}

    def __init__(
        self,
        main_template: PromptTemplate,
        pipeline_prompts: Mapping[str, PromptTemplate],
        *,
        name: str | None = None,
    ) -> None:
        self.main_template = main_template
        self.pipeline_prompts = dict(pipeline_prompts)
        self.name = name or f"pipeline_prompt:{id(self):x}"
        # 主模板中的 {key[:var]} 语法：key 指命名模板，var 指其输出
        self.reads: set[str] = set()
        for key in self.pipeline_prompts:
            self.reads |= self.pipeline_prompts[key].reads

    @property
    def id(self) -> str:
        return f"pipeline_prompt:{self.name}"

    def invoke(self, state: State) -> State:
        rendered: dict[str, str] = {}
        for key, tmpl in self.pipeline_prompts.items():
            rendered[key] = tmpl.format(state)
        main = self.main_template.format({**state, **rendered})
        return {"prompt": main}