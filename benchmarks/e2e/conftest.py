"""e2e 套件 pytest 锚点 + 共享 fixture。

- 使本目录进 sys.path（rootdir 外，支持 `from e2e_util import ...`）。
- `e2e_host`：DriverHost 真子进程 + 临时 sqlite db（跨语言 e2e 执行体）。
- `e2e_db`：临时 sqlite db 路径（重启恢复类测试用）。

运行（沿用 realworld 惯例）：
    uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/ -q
"""

from __future__ import annotations

from pathlib import Path

import pytest
from reactivegraph import DriverHost


@pytest.fixture
def e2e_db(tmp_path: Path) -> Path:
    """临时 sqlite 持久化 db（Driver 全后端：checkpoint/log/store/vector）。"""
    return tmp_path / "e2e.db"


@pytest.fixture
def e2e_host(e2e_db: Path):
    """DriverHost 真子进程：REACTIVEGRAPH_DB 指向临时 sqlite，start/handshake/close。"""
    host = DriverHost(env={"REACTIVEGRAPH_DB": str(e2e_db)})
    host.start()
    host.handshake()
    try:
        yield host
    finally:
        host.close()


@pytest.fixture
def e2e_host_memory():
    """DriverHost 无持久化（内存后端）对照。"""
    host = DriverHost(env={})
    host.start()
    host.handshake()
    try:
        yield host
    finally:
        host.close()
