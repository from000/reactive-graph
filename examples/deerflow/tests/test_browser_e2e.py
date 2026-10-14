"""真实浏览器端到端测试（deerflow 前端工作台，headless Chromium）。

用 playwright 打开工作台（真实 server + 真实 Node Driver），断言 DOM 渲染：
- 对话 tab：输入研究问题 → values 帧流渲染（选择性执行可视化）
- 能力实验室：scope 隔离 / dot 导出 / 向量检索 / HITL 卡片真实输出落 DOM

运行：
    uv run --directory examples/deerflow --extra test pytest tests/test_browser_e2e.py -q

skip 条件：node 或 packages/driver/dist 不可用（与 test_frontend.py 同策略）。
"""

from __future__ import annotations

import os
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest
from playwright.sync_api import Page, expect

REPO_ROOT = Path(__file__).resolve().parents[3]

needs_driver = pytest.mark.skipif(
    shutil.which("node") is None
    or not (REPO_ROOT / "packages" / "driver" / "dist" / "main.js").exists(),
    reason="bundled Driver (node + packages/driver/dist) not available",
)

pytestmark = needs_driver


@pytest.fixture(scope="module")
def server_base() -> str:
    """起一个真实工作台 server（真 Driver），模块内共享。"""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = Path(__file__).resolve().parents[1] / "frontend" / "server.py"
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(
        REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    )
    env["REACTIVEGRAPH_NODE_BIN"] = shutil.which("node") or ""
    proc = subprocess.Popen(
        [sys.executable, str(server), "--port", str(port)],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 20
    # 禁系统代理：localhost 请求直连（macOS 代理 127.0.0.1:7892 会把
    # 本机请求转发到代理 → 超时；与 test_frontend.py 同策略）。
    no_proxy = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    while time.time() < deadline:
        try:
            probe = urllib.request.Request(
                base + "/api/invoke", data=b"{}",
                headers={"Content-Type": "application/json"},
            )
            no_proxy.open(probe, timeout=2)
            break
        except Exception:  # noqa: BLE001 - server still starting
            time.sleep(0.3)
    else:
        proc.terminate()
        _out, err = proc.communicate(timeout=5)
        raise RuntimeError(
            f"workbench server did not start: {err.decode()[-2000:] if err else '(no stderr)'}"
        )
    try:
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _lab_card(page: Page, heading_text: str):
    """能力实验室中按 heading 定位卡片（执行按钮在 heading 的直接父容器内）。"""
    heading = page.get_by_role("heading", name=re.compile(heading_text))
    return heading.locator("xpath=..")


def test_home_page_renders(page: Page, server_base: str) -> None:
    """首页骨架：标题 / 导航 / 输入框渲染。"""
    page.goto(server_base + "/")
    expect(page).to_have_title(re.compile("ReactiveGraph"))
    expect(page.get_by_role("button", name="对话")).to_be_visible()
    expect(page.get_by_role("button", name="能力实验室")).to_be_visible()
    expect(page.get_by_role("textbox")).to_be_visible()


def test_chat_renders_values_stream(page: Page, server_base: str) -> None:
    """对话 tab：输入问题 → values 帧流渲染（answer 含问题回显）。"""
    console_log: list[str] = []
    page.on("console", lambda m: console_log.append(f"{m.type}: {m.text}"))
    page.goto(server_base + "/")
    page.get_by_role("textbox").fill("what is reactive execution?")
    page.get_by_role("button", name=re.compile("运行")).click()
    try:
        # values 帧可能落在不可见判定的渲染节点——断言 body 文本包含即可
        expect(page.locator("body")).to_contain_text(
            "researching: what is reactive execution?", timeout=10000
        )
    except AssertionError:
        body = page.inner_text("body")
        raise AssertionError(
            "chat 流未渲染 — console:\n"
            + "\n".join(console_log[-10:])
            + "\nbody 尾部:\n"
            + body[-800:]
        ) from None


def test_lab_scope_card_renders(page: Page, server_base: str) -> None:
    """能力实验室 scope 卡片：tenantA/tenantB 同 key 隔离渲染。"""
    page.goto(server_base + "/")
    page.get_by_role("button", name="能力实验室").click()
    _lab_card(page, "scope 多租户隔离").get_by_role("button", name="执行").click()
    expect(page.get_by_text("tenantA", exact=False).first).to_be_visible(timeout=10000)
    expect(page.get_by_text("tenantB", exact=False).first).to_be_visible(timeout=10000)


def test_lab_dot_and_vector_cards(page: Page, server_base: str) -> None:
    """能力实验室 dot/vector 卡片：EXPORT_DOT 与向量检索结果落 DOM。"""
    page.goto(server_base + "/")
    page.get_by_role("button", name="能力实验室").click()
    _lab_card(page, "dot 导出").get_by_role("button", name="执行").click()
    expect(page.get_by_text("digraph", exact=False).first).to_be_visible(timeout=10000)
    _lab_card(page, "向量检索").get_by_role("button", name="执行").click()
    expect(page.get_by_text("alpha", exact=False).first).to_be_visible(timeout=10000)
    expect(page.get_by_text("beta", exact=False).first).to_be_visible(timeout=10000)


def test_lab_hitl_card(page: Page, server_base: str) -> None:
    """能力实验室 HITL 卡片：interrupt → resume 结果（approved）渲染。"""
    page.goto(server_base + "/")
    page.get_by_role("button", name="能力实验室").click()
    _lab_card(page, "human-in-the-loop").get_by_role("button", name="执行").click()
    expect(page.get_by_text("interrupted", exact=False).first).to_be_visible(timeout=10000)
    expect(page.get_by_text("approved", exact=False).first).to_be_visible(timeout=10000)