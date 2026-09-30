"""07 · LLM token-level streaming via a generator task callback.

A task function may be a generator: each yielded value is streamed to
remote stream() consumers as a transient `messages` event before the task
commits. This gives a typewriter effect for LLM apps without extra wiring.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

# Ensure the local driver dist is used when this script is run from the repo.
repo_root = Path(__file__).resolve().parents[3]
os.environ.setdefault("REACTIVEGRAPH_DRIVER_ENTRY", str(repo_root / "packages" / "driver" / "dist" / "main.js"))
node = shutil.which("node")
if node is None:
    sys.exit("node not found; run `pnpm --filter @reactivegraph/driver build` first")
os.environ.setdefault("REACTIVEGRAPH_NODE_BIN", node)

from reactivegraph.graph import GraphBuilder, ReactiveGraph  # noqa: E402
from reactivegraph.host import DriverHost  # noqa: E402


def build(b: GraphBuilder) -> None:
    def llm(state):
        # a fake model: yields tokens, then commits a full TaskSuccess shape
        for token in ["Reactive", "Graph", " ", "streams", " tokens", "!"]:
            yield token
        return {
            "patches": [{"path": ["answer"], "operation": "set", "value": "ReactiveGraph streams tokens!"}],
            "writes": ["answer"],
            "return_value": "ReactiveGraph streams tokens!",
            "external_receipts": [],
        }

    b.task("llm", fn=llm).on("run", "llm")


def main() -> None:
    host = DriverHost()
    host.start()
    host.handshake()
    try:
        graph = ReactiveGraph.build(build, host=host, graph_id="g07")
        print("streaming run (watch the typewriter):")
        final_state = None
        for event in graph.stream("run", {}):
            et = event.get("eventType")
            if et == "messages":
                print(event["payload"]["message"]["content"], end="", flush=True)
            elif et == "values":
                final_state = event["payload"]["state"]
        print()
        print("final state:", final_state)
        assert final_state == {"answer": "ReactiveGraph streams tokens!"}
    finally:
        host.close()


if __name__ == "__main__":
    main()
