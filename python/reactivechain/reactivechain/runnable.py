"""Runnable 抽象：声明式管道段的基类与基础实现。

设计要点（docs/plans/2026-09-15-reactivechain-design.md §3）：
- 每段声明 `reads` / `writes`（顶层状态键集合）；
- `invoke(state)` 接收**完整状态 dict**，返回本段写入的键值 dict；
- 段级选择性执行：输入指纹不变即跳过段调用（进程内缓存，见 Pipeline）。

注：Python 侧 RGP/1 协议暂不支持 computed/段间事件路由（协议无 COMPUTE
消息），段级选择性在进程内实现；图集成时管道包装为单个 effect task。
"""

from __future__ import annotations

import asyncio
import copy
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

State = dict[str, Any]


class ReactiveChainError(RuntimeError):
    """ReactiveChain 用户错误（携带 Hint 式提示，与仓库错误风格一致）。"""


class Runnable(ABC):
    """管道段基类。子类实现 `invoke`；`reads`/`writes` 声明状态键。"""

    reads: set[str] = set()
    writes: set[str] = set()
    # 确定性段标记（方案 C 决策 R1）：pure=True 的段在编译为图时映射
    # kind="pure"，获得引擎输入指纹跳过；effect 段不跳过（有副作用语义）。
    pure: bool = False

    @property
    def id(self) -> str:
        return f"{type(self).__name__}@{id(self):x}"

    @abstractmethod
    def invoke(self, state: State) -> State:
        """接收完整状态 dict，返回本段写入的键值 dict。"""

    def stream(self, state: State, *, mode: str = "values") -> Iterator[State]:
        """默认无流式：一次性产出 invoke 结果。生成器段可覆写。

        `mode`（P3-1 Task 2）仅 Pipeline 层生效（values/updates/messages）；
        普通段无 chunk 流，忽略该参数。
        """
        yield self.invoke(state)

    def batch(self, inputs: list[State]) -> list[State]:
        return [self.invoke(i) for i in inputs]

    # -- async 面（便利性对齐 langchain 方法集；核心价值仍是同步状态契约）--

    async def ainvoke(self, state: State) -> State:
        """异步 invoke：executor 线程执行同步实现，不阻塞事件循环。"""
        return await asyncio.to_thread(self.invoke, state)

    async def astream(self, state: State, *, mode: str | None = None) -> AsyncIterator[State]:
        """异步流式：包装同步 stream 生成器（chunk 逐块产出）。

        `mode`（P3-1 Task 2）：透传给 `stream(mode=...)`；None = 旧行为。
        """
        for chunk in await asyncio.to_thread(self._collect_stream, state, mode):
            yield chunk

    def _collect_stream(self, state: State, mode: str | None = None) -> list[State]:
        if mode is None:
            return list(self.stream(state))
        return list(self.stream(state, mode=mode))

    async def abatch(self, inputs: list[State]) -> list[State]:
        """异步批处理：顺序执行（保持段级缓存共享语义）。"""
        return [await self.ainvoke(i) for i in inputs]

    def pipe(self, *others: Runnable) -> Pipeline:
        return Pipeline((self, *others))

    def __or__(self, other: Runnable) -> Pipeline:
        return Pipeline((self, other))

    def __ror__(self, other: Any) -> Pipeline:
        # 支持 `str | prompt` 等左侧字面量组合（M2 提供 PromptTemplate 时生效）。
        from .prompt import PromptTemplate

        return Pipeline((PromptTemplate(str(other)), self))

    # -- 输入提取 ----------------------------------------------------------

    def _extract_input(self, state: State) -> State:
        """按 `reads` 声明从完整状态提取本段输入；未声明时接收完整状态。"""
        if not self.reads:
            return dict(state)
        missing = [k for k in self.reads if k not in state]
        if missing:
            raise ReactiveChainError(
                f"段 '{self._label}' 的 reads 缺少输入键 {missing} — "
                f"Hint: 前一段需写入这些键，或输入需包含它们"
            )
        return {k: state[k] for k in sorted(self.reads)}

    @property
    def _label(self) -> str:
        return getattr(self, "name", self.id)


class RunnableLambda(Runnable):
    """把普通函数包装为管道段。

    参数：
        fn: 接收输入 dict（按 reads 提取）、返回写键 dict 的函数。
        reads: 本段读取的状态键；缺省时接收完整状态（宽松模式）。
        writes: 本段写入的状态键；缺省时按 invoke 返回值键推断。
        name: 段名（错误信息与缓存键可读性）。
    """

    def __init__(
        self,
        fn: Callable[[State], State],
        *,
        reads: set[str] | tuple[str, ...] | list[str] | None = None,
        writes: set[str] | tuple[str, ...] | list[str] | None = None,
        name: str | None = None,
        pure: bool = False,
    ) -> None:
        self._fn = fn
        self.reads = set(reads or ())
        self.writes = set(writes or ())
        self.name = name or getattr(fn, "__name__", type(fn).__name__)
        self.pure = pure

    @property
    def id(self) -> str:
        return f"lambda:{self.name}"

    def invoke(self, state: State) -> State:
        inp = self._extract_input(state)
        out = self._fn(inp)
        if not isinstance(out, dict):
            raise ReactiveChainError(
                f"段 '{self.name}' 必须返回 dict（写键→值），实际返回 "
                f"{type(out).__name__} — Hint: 用返回 {{'key': value}} 的包装函数"
            )
        if self.writes and not set(out).issubset(self.writes):
            extra = set(out) - self.writes
            raise ReactiveChainError(
                f"段 '{self.name}' 写入了未声明的键 {sorted(extra)} — "
                f"Hint: 在 writes 中声明这些键，或去掉 writes 让其按返回值推断"
            )
        return out


class RunnablePassthrough(Runnable):
    """透传段：原样返回输入（用于 RunnableParallel 组装/旁路）。"""

    writes: set[str] = set()

    def invoke(self, state: State) -> State:
        return dict(state)


class GeneratorRunnable(Runnable):
    """生成器段：`stream()` 逐块产出；`invoke()` 返回生成器最终 return 值。

    子类实现 `generate(state)`（生成器）；yield 的值作为流式块输出，
    最终 `return` 的值作为本段写入的键值（dict 或标量）。
    """

    def invoke(self, state: State) -> State:
        gen = self.generate(state)
        final: Any = None
        while True:
            try:
                next(gen)
            except StopIteration as stop:
                final = stop.value
                break
        if isinstance(final, dict):
            return final
        return {} if final is None else {"output": final}

    def stream(self, state: State, *, mode: str = "messages") -> Iterator[State]:
        gen = self.generate(state)
        try:
            while True:
                chunk = next(gen)
                if isinstance(chunk, dict):
                    yield chunk
                else:
                    yield {"chunk": chunk}
        except StopIteration as stop:
            # 生成器的 return 值作为本段最终输出（供 Pipeline.stream 收集）。
            return stop.value

    def generate(self, state: State) -> Iterator[Any]:  # pragma: no cover - abstract
        raise NotImplementedError


class Pipeline(Runnable):
    """`A | B | C` 的产物：按序执行各段，段级指纹跳过。

    选择性：每段以 `(输入指纹 → 输出)` 缓存；相同输入再次出现时整段跳过
    （不调用段函数），直接复用缓存输出——与 ReactiveGraph driver 的 pure
    任务指纹跳过语义一致。单段 Runnable 直接调用无缓存；包装成 Pipeline
    后获得缓存（含 batch 批内共享）。
    """

    # 可观测钩子（instrument_pipeline 装配；普通管道保持默认）。
    _instrumented: bool = False
    _stats: Any = None
    _callback_manager: Any = None

    def __init__(self, segments: tuple[Runnable, ...] | list[Runnable]) -> None:
        if not segments:
            raise ReactiveChainError("Pipeline 至少需要一段 — Hint: chain = a | b")
        self.segments = list(segments)
        # 编译缓存：invoke 复用同一图对象（引擎 fallback 的 pure 指纹缓存
        # 跨 invoke 共享；to_graph 显式编译时重建）。见
        # docs/plans/2026-09-15-reactivechain-engine-runtime.md P0。
        self._compiled_graph: Any = None
        # 末段实际输出（wrapper 记录，引擎化前的链返回语义：返回末段
        # 输出的全部键，而非仅 declared writes）；skip 时保持上次缓存值。
        self._last_out: State | None = None
        # invoke 串行锁：并发 ainvoke 下 _last_out 读写一致（与旧
        # `_cache_lock` 语义等价；引擎 fallback 另有图级锁防重复执行）。
        self._invoke_lock = threading.Lock()

    @property
    def id(self) -> str:
        return "|".join(seg.id for seg in self.segments)

    @property
    def reads(self) -> set[str]:  # type: ignore[override]
        return set(self.segments[0].reads)

    @property
    def writes(self) -> set[str]:  # type: ignore[override]
        return set(self.segments[-1].writes)

    def clear_cache(self) -> None:
        """清空段级跳过缓存（引擎 fallback 的 pure 指纹缓存）。

        递归清子 Pipeline：`A|B|C | D` 组合产生嵌套 Pipeline（外层段是子
        Pipeline），子 Pipeline 的段级缓存必须一并清空（否则"强制全量"
        对照仍命中内层检索缓存——realworld measure_skip_vs_full 验证点）。
        """
        if self._compiled_graph is not None:
            self._compiled_graph._fallback_cache.clear()
        for seg in self.segments:
            if isinstance(seg, Pipeline):
                seg.clear_cache()

    def _segment_fn(self, i: int, seg: Runnable) -> Callable[[State], State]:
        """段 fn 的引擎侧包装：可观测钩子（统计/回调/链级 manager）内联。"""

        def wrapper(state: State) -> State:
            stats: Any = self._stats
            mgr: Any = self._callback_manager
            inp = seg._extract_input(state)
            if mgr is not None:
                mgr.emit("on_chain_start", seg, inp)
            start = time.perf_counter()
            try:
                last_out = seg.invoke(state)
            except Exception as exc:  # noqa: BLE001 - 事件透传后重抛
                if mgr is not None:
                    mgr.emit("on_chain_error", seg, exc)
                raise
            if mgr is not None:
                mgr.emit("on_chain_end", seg, last_out,
                         (time.perf_counter() - start) * 1000)
            if stats is not None:
                stats.record_call(seg.id, (time.perf_counter() - start) * 1000)
            # 链返回语义：末段实际输出（全部键，非仅 declared writes）。
            self._last_out = last_out
            return last_out

        return wrapper

    def _on_segment_skipped(self, tid: str) -> None:
        """引擎 pure 指纹跳过的统计钩子（skip 段不执行 → 不经过 wrapper）。"""
        stats: Any = self._stats
        if stats is None:
            return
        idx = int(tid.split("_", 1)[1])
        stats.record_skip(self.segments[idx].id)

    def _build_graph(
        self, *, graph_id: str | None = None, pure_segments: set[str] | None = None,
        computed_segments: set[str] | None = None, host: Any = None,
    ) -> Any:
        """把管道编译为 ReactiveGraph 图（单一编译入口，invoke/to_graph 共用）。

        每段一个 task（段级调度）：`kind` 由段声明 `seg.pure` 或
        `pure_segments`（段 id 形如 "seg_0"）决定——pure 段获得引擎输入
        指纹跳过（决策 R1：仅 pure 段跳过）。fn 为可观测包装（统计/回调/
        链级 manager 事件），skip 经 `_skip_callback` 上报统计。

        `computed_segments`（P1-2）：把单写键的纯派生段编译为引擎 computed
        节点——读路径哈希缓存（键未变化不重算 selector）。不参与统计/回调
        包装（派生表达式语义）；链 invoke 返回末段写键时对 computed 段
        从引擎 state 提取。
        """
        from reactivegraph import ReactiveGraph

        gid = graph_id or f"chain_{self.id}"
        pure = pure_segments or set()
        computed = computed_segments or set()
        self._computed_tids: set[str] = set()

        def build(b: Any) -> None:
            for i, seg in enumerate(self.segments):
                tid = f"seg_{i}"
                if tid in computed:
                    if len(seg.writes) != 1:
                        raise ReactiveChainError(
                            f"computed 段 '{tid}' 必须恰好一个写键，实际 {sorted(seg.writes)} "
                            f"— Hint: computed 是单值派生表达式（键变化自动重算）"
                        )
                    if not seg.pure:
                        raise ReactiveChainError(
                            f"computed 段 '{tid}' 需要 pure=True（确定性派生）"
                        )
                    write_key = next(iter(seg.writes))

                    def selector(state: State, seg: Runnable = seg,
                                 key: str = write_key) -> Any:
                        out = seg.invoke(dict(state))
                        if not isinstance(out, dict):
                            return None
                        return out.get(key)

                    self._computed_tids.add(tid)
                    # 引擎约定：computed 值写入 state[computed_id] → id 用写键
                    b.computed(write_key, selector, reads=tuple(sorted(seg.reads)))
                    continue
                # 段路由统一 "run"：fallback（事件传播，注册顺序）与真实
                # Driver（routeFor("run") 顺序执行 entry 任务集）双执行器
                # 一致——Driver 当前版本无 `{tid}:written` 事件机制
                # （runtime.run 只执行 entry 事件路由的任务，无下游级联），
                # 订阅链编译会令 Driver 只跑首段。written 订阅与 computed
                # 自动求值为 fallback 扩展能力（用户 GraphBuilder 显式构建
                # 时可用），链编译保持全 "run" 保证一致（见方案 C §5 P1
                # 与 docs/plans/2026-09-15-reactivechain-engine-runtime.md）。
                b.task(
                    tid,
                    kind="pure" if (tid in pure or seg.pure) else "effect",
                    fn=self._segment_fn(i, seg),
                    on=("run",),
                    writes=tuple(sorted(seg.writes)),
                )

        graph = ReactiveGraph.build(build, graph_id=gid, host=host)
        graph._skip_callback = self._on_segment_skipped
        return graph

    def invoke(self, state: State) -> State:
        # 执行移交引擎（单一执行路径，见 docs/plans/2026-09-15-...md P0）：
        # 统计/回调为可观测钩子（instrument_pipeline），经 _segment_fn 包装
        # 内联在段边界，被 pure 指纹跳过的段经 _skip_callback 记 skipped。
        stats = None
        mgr = None
        token = None
        if self._instrumented:
            from .callback import ChainStats  # 延迟导入避免模块循环

            stats = ChainStats()
            self._stats = stats
            mgr = self._callback_manager
            if mgr is not None:
                # 链级 manager 设为当前上下文：子段（llm/tool/retry）的
                # 组件事件经 get_callback_manager() 走本链 manager。
                from .callback import _current_mgr

                token = _current_mgr.set(mgr)
        try:
            if self._compiled_graph is None:
                self._compiled_graph = self._build_graph()
            if stats is not None:
                stats.started_at = time.perf_counter()
            # 执行 + 读取 _last_out 持链级锁：并发 ainvoke 下读写一致。
            with self._invoke_lock:
                out_state = self._compiled_graph.invoke("run", dict(state))
                # 深拷贝返回：调用方修改返回 dict 不污染内部 _last_out
                # 与引擎 state（引擎 fallback 缓存另行 deepcopy，见 P0-1）。
                out = copy.deepcopy(self._last_out) if self._last_out is not None else {}
                # 末段为 computed（无 wrapper，_last_out 不更新）：从引擎
                # state 提取其写键（P1-2 派生键）。
                last_tid = f"seg_{len(self.segments) - 1}"
                if last_tid in self._computed_tids and self.segments[-1].writes:
                    for k in self.segments[-1].writes:
                        if k in out_state:
                            out[k] = out_state[k]
            if stats is not None:
                stats.finished_at = time.perf_counter()
            return out
        finally:
            if token is not None:
                from .callback import _current_mgr

                _current_mgr.reset(token)

    def stream(self, state: State, *, mode: str = "values") -> Iterator[State]:
        """统一流式（P3-1 Task 2，design §3）。

        - ``values``（默认）：引擎事件流的全 state 快照帧（末帧含最终结果）；
        - ``updates``：段写键增量（``{seg_id: writes}``）；
        - ``messages``：LLM/生成器段原始 chunk（``{"chunk": ...}``）逐块产出，
          不双重执行（C1）。

        values/updates 经引擎 ``graph.stream``（fallback 事件流 / Driver
        ``run_stream``），与 invoke 共用单一编译产物；未知 mode 抛
        ``ReactiveChainError``。
        """
        if mode not in ("values", "updates", "messages"):
            raise ReactiveChainError(
                f"unknown stream mode {mode!r} — 支持 values|updates|messages"
            )
        if mode == "messages":
            yield from self._stream_messages(state)
            return
        if self._compiled_graph is None:
            self._compiled_graph = self._build_graph()
        for ev in self._compiled_graph.stream("run", dict(state)):
            etype = ev.get("eventType")
            if mode == "values" and etype == "values":
                yield ev["payload"]["state"]
            elif mode == "updates" and etype == "task_end":
                yield {ev["task"]: ev.get("writes", {})}

    def _stream_messages(self, state: State) -> Iterator[State]:
        """messages 模式：段级 chunk 流（旧语义，C1：不双重执行）。"""
        cur = dict(state)
        last_out: State = {}
        for seg in self.segments:
            inp = seg._extract_input(cur)
            if type(seg).stream is not Runnable.stream:
                # 覆写 stream 的段（LLM/生成器）：消费其流式块，并以
                # StopIteration.value 收集最终输出，避免二次 invoke 双重执行。
                it = seg.stream(inp)
                try:
                    while True:
                        chunk = next(it)
                        if chunk is not None:
                            yield chunk
                except StopIteration as stop:
                    # 归一化与 GeneratorRunnable.invoke 一致：dict 原样，
                    # 标量 → {"output": ...}（下游段输入不因路径分叉）。
                    final = stop.value
                    if isinstance(final, dict):
                        last_out = final
                    elif final is not None:
                        last_out = {"output": final}
                    else:
                        last_out = {}
            else:
                last_out = seg.invoke(inp)
            cur.update(last_out)
        return

    def batch(self, inputs: list[State]) -> list[State]:
        """批处理：共享段缓存，相同输入只计算一次（批内选择性）。"""
        return [self.invoke(i) for i in inputs]

    def to_graph(
        self, *, graph_id: str | None = None,
        pure_segments: set[str] | None = None,
        computed_segments: set[str] | None = None,
        host: Any = None,
    ) -> Any:
        """把管道编译为 ReactiveGraph 图：**每段一个 task**（段级调度）。

        与 `invoke` 共用单一编译入口（`_build_graph`）；段 `pure` 声明或
        `pure_segments`（段 id 集合，形如 `"seg_0"`）决定 pure 段——获得
        引擎输入指纹跳过（选择性，远超 langchain 的差异化能力），**仅限
        确定性、无副作用的段**（PromptTemplate/纯函数段），LLM/IO 段不可
        标记。`computed_segments`（P1-2）：单写键 pure 段编译为引擎 computed
        派生节点（读路径哈希缓存，键未变化不重算）。返回可
        `graph.invoke("run", state)` 的 `ReactiveGraph`。

        `host`（P2-1）：传入 `DriverHost` 后图走真实 Driver 执行——
        链级管道在 Driver 任务级联下与 fallback 输出一致，且可经返回的
        graph 使用引擎持久化能力（`get_state`/`list_checkpoints`/
        `restore_thread`/`store_put`/`store_get`）。未传则 in-process
        fallback（链默认执行路径）。
        """
        return self._build_graph(
            graph_id=graph_id,
            pure_segments=pure_segments,
            computed_segments=computed_segments,
            host=host,
        )
