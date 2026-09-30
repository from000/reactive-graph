"""DeerFlow 移植套件 pytest 锚点 + 共享 fixture。

- 使本目录（benchmarks/deerflow/）进 sys.path，支持 `from deerflow_port import ...`
  （移植实现非安装包，须显式插入）。
- `df_db`：临时 sqlite db 路径（持久化/重启恢复类测试用）。
- `df_host`：DriverHost 真子进程 + 临时 sqlite（跨语言 e2e 执行体）。
- `df_host_memory`：DriverHost 内存后端对照。

运行（沿用 e2e/realworld 惯例）：
    uv run --directory python/reactivechain pytest $PWD/benchmarks/deerflow/ -q
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from reactivegraph import DriverHost

# benchmarks/deerflow/ → sys.path（tests/ 的父目录）
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture
def df_db(tmp_path: Path) -> Path:
    """临时 sqlite 持久化 db（Driver 全后端：checkpoint/log/store/vector）。"""
    return tmp_path / "deerflow.db"


@pytest.fixture
def df_host(df_db: Path):
    """DriverHost 真子进程：REACTIVEGRAPH_DB 指向临时 sqlite，start/handshake/close。"""
    host = DriverHost(env={"REACTIVEGRAPH_DB": str(df_db)})
    host.start()
    host.handshake()
    try:
        yield host
    finally:
        host.close()


@pytest.fixture
def df_host_memory():
    """DriverHost 无持久化（内存后端）对照。"""
    host = DriverHost(env={})
    host.start()
    host.handshake()
    try:
        yield host
    finally:
        host.close()
