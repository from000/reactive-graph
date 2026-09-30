"""08 · Real LLM token streaming with only the standard library.

A minimal, reusable pattern for OpenAI-compatible gateways (AGNES, any
OpenAI-shaped /v1/chat/completions endpoint): urllib + SSE line parsing,
one token per yield. No third-party deps.

Environment (or shell profile):
    export AGNES_API_KEY=...                        # or your gateway key
    export CUSTOM_LLM_URL=https://apihub.agnes-ai.com/v1
    export CUSTOM_MODEL=agnes-2.5-flash

Without AGNES_API_KEY the script prints how to configure it and exits 0
(so tutorial checks stay green); with it, it streams tokens live.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator
from urllib import request


def completion_stream(messages: list[dict]) -> Iterator[str]:
    """Yield content tokens from an OpenAI-compatible chat.completions stream."""
    key = os.environ.get("AGNES_API_KEY", "")
    base = os.environ.get("CUSTOM_LLM_URL", "https://apihub.agnes-ai.com/v1").rstrip("/")
    model = os.environ.get("CUSTOM_MODEL", "agnes-2.5-flash")
    req = request.Request(
        f"{base}/chat/completions",
        data=json.dumps({"model": model, "messages": messages, "stream": True}).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {key}",
            "Accept": "text/event-stream",
        },
    )
    with request.urlopen(req, timeout=120) as r:  # noqa: S310 - explicit gateway URL
        for raw in r:
            line = raw.decode("utf-8", "replace").strip()
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                break
            try:
                obj = json.loads(data)
            except json.JSONDecodeError:
                continue
            delta = (obj.get("choices") or [{}])[0].get("delta") or {}
            content = delta.get("content")
            if content:
                yield content


def main() -> None:
    if not os.environ.get("AGNES_API_KEY"):
        print(
            "AGNES_API_KEY not set — configure it to stream a real model:\n"
            "  export AGNES_API_KEY=...\n"
            "  export CUSTOM_LLM_URL=https://apihub.agnes-ai.com/v1\n"
            "  export CUSTOM_MODEL=agnes-2.5-flash"
        )
        return

    question = sys.argv[1] if len(sys.argv) > 1 else "用一句话介绍 ReactiveGraph"
    print(f"Q: {question}\nA: ", end="", flush=True)
    parts: list[str] = []
    for token in completion_stream([{"role": "user", "content": question}]):
        parts.append(token)
        print(token, end="", flush=True)
    print("\n\n[streamed", len(parts), "tokens; joined length", len("".join(parts)), "chars]")


if __name__ == "__main__":
    main()
