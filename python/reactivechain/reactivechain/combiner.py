"""组合算子（对应 langchain_core.runnables 组合面）。

- `RunnableParallel`：多分支并行（进程内顺序执行、语义并行）；
- `RunnableBranch`：条件路由（谓词 → 分支）；
- `RunnableFallback`：主链异常回退；
- `RunnableAssign`：透传 + 注入字段（RunnablePassthrough.assign 等价）；
- `Runnable` 扩展方法：`with_fallbacks` / `with_retry` / `map`。

组合器输出语义与 LangChain 一致（分支结果嵌套在分支名下）。
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence

from .callback import get_callback_manager
from .runnable import Pipeline, ReactiveChainError, Runnable, State


class RunnableParallel(Runnable):
    """并行分支：{name: runnable} → invoke 后 {name: 分支输出}。"""

    def __init__(self, branches: dict[str, Runnable] | None = None, **kwargs: Runnable) -> None:
        self.branches = dict(branches or {})
        self.branches.update(kwargs)
        if not self.branches:
            raise ReactiveChainError(
                "RunnableParallel 至少需要一个分支 — Hint: 传入 {name: runnable}"
            )

    @property
    def id(self) -> str:
        return "parallel{" + ",".join(self.branches) + "}"

    @property
    def reads(self) -> set[str]:  # type: ignore[override]
        out: set[str] = set()
        for b in self.branches.values():
            out |= set(getattr(b, "reads", set()))
        return out

    @property
    def writes(self) -> set[str]:  # type: ignore[override]
        return set(self.branches)

    def invoke(self, state: State) -> State:
        out: State = {}
        for name, runnable in self.branches.items():
            out[name] = runnable.invoke(state)
        return out

    def __or__(self, other: Runnable) -> Pipeline:
        return Pipeline((self, other))


class RunnableBranch(Runnable):
    """条件路由：[(谓词, 分支), ...] + default。"""

    def __init__(
        self,
        branches: Sequence[tuple[Callable[[State], bool], Runnable]],
        default: Runnable,
    ) -> None:
        if not branches:
            raise ReactiveChainError(
                "RunnableBranch 至少需要一个分支 — Hint: [(pred, runnable), ...]"
            )
        self.branches = list(branches)
        self.default = default

    @property
    def id(self) -> str:
        return f"branch({len(self.branches)}+default)"

    @property
    def reads(self) -> set[str]:  # type: ignore[override]
        out: set[str] = set()
        for _, b in self.branches:
            out |= set(getattr(b, "reads", set()))
        out |= set(getattr(self.default, "reads", set()))
        return out

    @property
    def writes(self) -> set[str]:  # type: ignore[override]
        # 声明 = 全部分支（含 default）写键并集：任意分支可能被选中执行。
        out = set(getattr(self.default, "writes", set()))
        for _, b in self.branches:
            out |= set(getattr(b, "writes", set()))
        return out

    def _pick(self, state: State) -> Runnable:
        for pred, runnable in self.branches:
            try:
                if pred(state):
                    return runnable
            except Exception as exc:  # noqa: BLE001
                raise ReactiveChainError(
                    f"RunnableBranch 谓词异常：{exc} — Hint: 谓词应返回 bool"
                ) from exc
        return self.default

    def invoke(self, state: State) -> State:
        return self._pick(state).invoke(state)


class RunnableFallback(Runnable):
    """异常回退：主链失败依次尝试 fallback 链。"""

    def __init__(self, runnable: Runnable, *fallbacks: Runnable) -> None:
        self.runnable = runnable
        self.fallbacks = list(fallbacks)

    @property
    def id(self) -> str:
        return f"fallback({self.runnable.id}+{len(self.fallbacks)})"

    @property
    def reads(self) -> set[str]:  # type: ignore[override]
        out = set(getattr(self.runnable, "reads", set()))
        for f in self.fallbacks:
            out |= set(getattr(f, "reads", set()))
        return out

    @property
    def writes(self) -> set[str]:  # type: ignore[override]
        return set(getattr(self.runnable, "writes", set()))

    def invoke(self, state: State) -> State:
        attempts: list[Runnable] = [self.runnable, *self.fallbacks]
        last: Exception | None = None
        for attempt in attempts:
            try:
                return attempt.invoke(state)
            except Exception as exc:  # noqa: BLE001
                last = exc
        assert last is not None
        raise last


class RunnableAssign(Runnable):
    """透传 + 注入字段（RunnablePassthrough.assign 等价）。"""

    def __init__(self, fields: dict[str, Runnable] | None = None, **kwargs: Runnable) -> None:
        self.fields = dict(fields or {})
        self.fields.update(kwargs)

    @property
    def id(self) -> str:
        return "assign{" + ",".join(self.fields) + "}"

    @property
    def reads(self) -> set[str]:  # type: ignore[override]
        out: set[str] = set()
        for f in self.fields.values():
            out |= set(getattr(f, "reads", set()))
        return out

    @property
    def writes(self) -> set[str]:  # type: ignore[override]
        # 声明 = 字段名 ∪ 子段实际写键（invoke 扁平合并子段输出，见下）。
        out = set(self.fields)
        for f in self.fields.values():
            out |= set(getattr(f, "writes", set()))
        return out

    def invoke(self, state: State) -> State:
        out = dict(state)
        for name, runnable in self.fields.items():
            result = runnable.invoke(state)
            if isinstance(result, dict):
                out.update(result)  # 段输出为写键 dict → 扁平合并
            else:
                out[name] = result
        return out


class _RetryWrapper(Runnable):
    """with_retry 实现：异常重试 + 退避。"""

    def __init__(
        self,
        runnable: Runnable,
        *,
        max_attempts: int = 3,
        retry_if: type[Exception] | tuple[type[Exception], ...] = Exception,
        backoff_s: float = 0.0,
    ) -> None:
        self.runnable = runnable
        self.max_attempts = max_attempts
        self.retry_if = retry_if
        self.backoff_s = backoff_s

    @property
    def id(self) -> str:
        return f"retry({self.runnable.id})"

    @property
    def reads(self) -> set[str]:  # type: ignore[override]
        return set(getattr(self.runnable, "reads", set()))

    @property
    def writes(self) -> set[str]:  # type: ignore[override]
        return set(getattr(self.runnable, "writes", set()))

    def invoke(self, state: State) -> State:
        last: Exception | None = None
        for attempt in range(self.max_attempts):
            try:
                return self.runnable.invoke(state)
            except Exception as exc:  # noqa: BLE001
                last = exc
                if not isinstance(exc, self.retry_if):
                    raise
                get_callback_manager().emit("on_retry", self, attempt, exc)
                if attempt < self.max_attempts - 1 and self.backoff_s:
                    time.sleep(self.backoff_s * (attempt + 1))
        assert last is not None
        raise last


def _with_fallbacks(self: Runnable, fallbacks: Sequence[Runnable]) -> RunnableFallback:
    return RunnableFallback(self, *fallbacks)


def _with_retry(
    self: Runnable,
    *,
    max_attempts: int = 3,
    retry_if_exception_type: type[Exception] | tuple[type[Exception], ...] = Exception,
    backoff_s: float = 0.0,
) -> Runnable:
    return _RetryWrapper(
        self,
        max_attempts=max_attempts,
        retry_if=retry_if_exception_type,
        backoff_s=backoff_s,
    )


def _map(self: Runnable, *, max_batch_size: int | None = None) -> Runnable:
    """批处理包装：invoke 变 batch（LangChain Runnable.map 语义）。"""
    size = max_batch_size or 16

    class _MapBatch(Runnable):
        @property
        def id(self) -> str:
            return f"map({self._inner.id})"

        def __init__(self, inner: Runnable) -> None:
            self._inner = inner
            self.reads = set(getattr(inner, "reads", set()))
            self.writes = set(getattr(inner, "writes", set()))

        def invoke(self, state: State) -> State:
            if "items" not in state:
                raise ReactiveChainError("map 段缺少输入键 'items' — Hint: 提供批输入列表")
            items = state["items"]
            if not isinstance(items, list):
                raise ReactiveChainError("map 段需要输入键 'items'（列表）— Hint: 提供批输入")
            outs = []
            for i in range(0, len(items), size):
                outs.extend(self._inner.batch(items[i : i + size]))
            return {"items_out": outs}

    return _MapBatch(self)


# 注册扩展方法到 Runnable
Runnable.with_fallbacks = _with_fallbacks  # type: ignore[attr-defined]
Runnable.with_retry = _with_retry  # type: ignore[attr-defined]
Runnable.map = _map  # type: ignore[attr-defined]
Runnable.assign = (  # type: ignore[attr-defined]
    lambda self, **fields: Pipeline((self, RunnableAssign(fields)))
)