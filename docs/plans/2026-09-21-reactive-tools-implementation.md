# Reactive Tools Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Upgrade ReactiveChain tools with an industry-standard tool protocol and Reactive state/effect contracts.

**Architecture:** Extend the existing `reactivechain.tools` module in-place. Add small typed runtime primitives (`ToolResult`, injection markers, metadata) without introducing external dependencies. Preserve the existing state-dict contract and agent tool-call loop.

**Tech Stack:** Python 3.10+, pytest, ReactiveChain, ReactiveGraph ToolSpec.

---

### Task 1: ToolResult and metadata
**Files:**
- Modify: `python/reactivechain/reactivechain/tools.py`
- Test: `python/reactivechain/tests/test_tools.py`

**Steps:**
1. Add failing tests for `ToolResult`, `@tool(reads/writes/kind/return_direct/on_error)` metadata.
2. Run `uv run --directory python/reactivechain pytest tests/test_tools.py -q`; confirm metadata tests fail.
3. Implement dataclass and decorator parameters.
4. Re-run tests; expected pass.
5. Commit: `feat(tools): add reactive tool metadata and ToolResult`.

### Task 2: Injected state and tool call id
**Files:**
- Modify: `python/reactivechain/reactivechain/tools.py`
- Test: `python/reactivechain/tests/test_tools.py`

**Steps:**
1. Add failing tests: injected parameters omitted from schema, state/tool_call_id injected during invoke.
2. Confirm RED.
3. Implement injection markers and runtime injection.
4. Confirm GREEN.
5. Commit: `feat(tools): inject state and tool call id`.

### Task 3: Artifact and patches
**Files:**
- Modify: `python/reactivechain/reactivechain/tools.py`
- Test: `python/reactivechain/tests/test_tools.py`

**Steps:**
1. Add failing tests: `ToolResult.content` becomes model content, artifact retained separately, patches merged into output.
2. Confirm RED.
3. Implement normalization.
4. Confirm GREEN.
5. Commit: `feat(tools): support tool artifacts and state patches`.

### Task 4: Error policy and async
**Files:**
- Modify: `python/reactivechain/reactivechain/tools.py`
- Test: `python/reactivechain/tests/test_tools.py`

**Steps:**
1. Add failing tests: `on_error="raise"` raises, default captures; `_arun` invokes `_run`.
2. Confirm RED.
3. Implement async wrapper and error policy.
4. Confirm GREEN.
5. Commit: `feat(tools): add async execution and error policy`.

### Task 5: return_direct integration
**Files:**
- Modify: `python/reactivechain/reactivechain/agents.py`
- Test: `python/reactivechain/tests/test_agents.py`

**Steps:**
1. Add failing test: a tool with `return_direct=True` causes `_ToolCallingLoopAgent` to return immediately.
2. Confirm RED.
3. Propagate return_direct through tool output and agent loop.
4. Confirm GREEN.
5. Commit: `feat(tools): support return_direct in tool-calling agent`.

### Task 6: Safe terminal default
**Files:**
- Modify: `python/reactivechain/reactivechain/tools.py`
- Test: `python/reactivechain/tests/test_tools.py`

**Steps:**
1. Add failing test: terminal rejects by default and allows explicit policy.
2. Confirm RED.
3. Implement `TerminalPolicy`.
4. Confirm GREEN.
5. Commit: `fix(tools): require explicit terminal policy`.

### Task 7: Regression and docs
**Files:**
- Modify: `docs/reactivechain-langchain-comparison.md`
- Modify: `python/reactivechain/README.md`

**Steps:**
1. Run reactivechain tests.
2. Run reactivegraph and DeerFlow regression.
3. Run ruff/mypy.
4. Update docs with implemented capability and honest remaining gaps.
5. Commit: `docs(tools): document reactive tool protocol`.
