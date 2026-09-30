# Toolchain Baseline

Recorded 2026-08-31 (see Appendix H of the implementation plan: record the initial toolchain versions before production code).

| Tool | Version | Notes |
|---|---|---|
| OS | macOS 26.6.1 (darwin/x86_64) | Build 25G76 |
| git | 2.39.3 (Apple Git-146) | repo initialized at workspace root |
| node | v22.23.2 | downloaded to `~/.toolchain/node`, added to PATH |
| npm | 10.9.8 | ships with node |
| pnpm | 11.24.0 | activated via corepack |
| uv | 0.12.7 | downloaded to `~/.toolchain/uvx`, added to PATH |
| python (uv-managed) | CPython 3.10.21 (macos-x86_64) | `uv python install 3.10` |

## Network note

Direct internet access is unreliable. All network operations run through the local
HTTP proxy `http://127.0.0.1:7892`, exported as `https_proxy`/`http_proxy`
(see `~/.toolchain/env.sh`).

## PATH

```bash
export PATH="$HOME/.toolchain/node/bin:$HOME/.toolchain/uvx/uv-x86_64-apple-darwin:$PATH"
```

## Upstream reference

Performance comparisons reference `langchain-ai/langgraph` (pin
`11ee185999b86bfea2d8c0e69cef9a5e37acf686`); measurement protocol and raw
numbers live in `docs/benchmarks.md`.
