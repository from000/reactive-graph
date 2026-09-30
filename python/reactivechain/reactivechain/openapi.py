"""OpenAPI 适配器：OpenAPI 3.x spec → ReactiveChain 工具段。

生态独立（不依赖 langchain/任何第三方框架）：OpenAPI/JSON Schema 是行业
通用规范，任意 OpenAPI 服务即工具源——"堆叠功能直接拿来"的协议化实现
（用户 2026-09-15 决策：弃 langchain 适配，改接 MCP/OpenAPI 通用规范）。
纯 stdlib：urllib 直连（同 llm/embeddings 网络层，禁系统代理坑）。
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .tools import BaseTool

_HTTP_METHODS = ("get", "post", "put", "patch", "delete")


def from_openapi(spec: dict[str, Any], *, base_url: str,
                 headers: dict[str, str] | None = None,
                 retries: int = 2, timeout_s: float = 30.0) -> list[BaseTool]:
    """OpenAPI 3.x spec → 工具段列表。

    - 工具名 = `operationId`（缺省 `{method}_{path}` 归一化，非法字符替换为 `_`）
    - `args_schema` = path/query/header 参数 + requestBody(application/json)
      递归合并；`required` = path 必填参数 + body schema 声明的必填项
    - invoke 构造 HTTP 请求：path 参数填充 URL、query/header 参数各归其位、
      POST/PUT/PATCH 剩余参数作 JSON body；直连（`ProxyHandler({})`），
      5xx 重试（4xx 不重试），错误统一抛 `ValueError`（含 code/body 摘要，
      由 BaseTool.invoke 转 Error 字符串回传 agent）

    参数：
        spec: 解析后的 OpenAPI 3.x spec dict（含 `paths`）。
        base_url: 服务根地址，如 `https://api.example.com`。
        headers: 附加请求头（如鉴权 `{"Authorization": "Bearer ..."}`）。
        retries: 5xx/网络错误的额外重试次数（默认 2，共最多 3 次尝试）。
        timeout_s: 单次请求超时秒数（默认 30）。
    """
    if not isinstance(spec, dict) or "paths" not in spec:
        raise ValueError(
            "from_openapi 需要 OpenAPI 3.x spec dict（含 'paths'） — Hint: 传入"
            "服务官方的 OpenAPI 文档 JSON"
        )
    tools: list[BaseTool] = []
    for path, item in (spec.get("paths") or {}).items():
        if not isinstance(item, dict):
            continue
        for method, op in item.items():
            if method not in _HTTP_METHODS or not isinstance(op, dict):
                continue
            tools.append(
                _OpenAPITool(
                    base_url=base_url.rstrip("/"),
                    headers=headers or {},
                    path=path,
                    method=method,
                    op=op,
                    retries=retries,
                    timeout_s=timeout_s,
                )
            )
    return tools


def _sanitize_name(raw: str) -> str:
    """operationId → 合法工具名（[A-Za-z0-9_]）。"""
    out = []
    for ch in raw:
        out.append(ch if ch.isalnum() or ch == "_" else "_")
    return "".join(out)


class _OpenAPITool(BaseTool):
    """单个 OpenAPI 操作的工具段（工厂生成，spec 字段闭包持有）。"""

    def __init__(self, *, base_url: str, headers: dict[str, str],
                 path: str, method: str, op: dict[str, Any],
                 retries: int = 2, timeout_s: float = 30.0) -> None:
        self.base_url = base_url
        self.headers = headers
        self.path = path
        self.method = method
        self.op = op
        self.retries = retries
        self.timeout_s = timeout_s
        op_id = op.get("operationId") or f"{method}_{path}"
        self.name = _sanitize_name(str(op_id))
        self.description = str(op.get("summary") or op.get("description") or "")
        self.args_schema = self._build_schema()

    # -- schema -----------------------------------------------------------

    def _build_schema(self) -> dict[str, Any]:
        props: dict[str, Any] = {}
        required: list[str] = []
        for p in self.op.get("parameters") or []:
            if not isinstance(p, dict) or not p.get("name"):
                continue
            # name 可能是非 str（spec 容错）
            name = str(p["name"])
            schema = p.get("schema") or {"type": "string"}
            if p.get("in") == "path" and p.get("required"):
                required.append(name)
            props[name] = schema
        body = self.op.get("requestBody") or {}
        content = (body.get("content") or {}).get("application/json") or {}
        body_schema = content.get("schema")
        if isinstance(body_schema, dict):
            sub = body_schema.get("properties") or {}
            props.update({str(k): v for k, v in sub.items() if isinstance(v, dict)})
            for name in body_schema.get("required") or []:
                if str(name) not in required:
                    required.append(str(name))
        return {"type": "object", "properties": props, "required": required}

    def _query_params(self) -> set[str]:
        return {
            str(p["name"])
            for p in (self.op.get("parameters") or [])
            if isinstance(p, dict) and p.get("in") == "query" and p.get("name")
        }

    def _header_params(self) -> set[str]:
        return {
            str(p["name"])
            for p in (self.op.get("parameters") or [])
            if isinstance(p, dict) and p.get("in") == "header" and p.get("name")
        }

    # -- 调用 -------------------------------------------------------------

    def _run(self, **kwargs: Any) -> Any:
        url = self.base_url + self.path
        for name in list(kwargs):  # path 参数填充（如 /items/{id}）
            if f"{{{name}}}" in url:
                url = url.replace(f"{{{name}}}", str(kwargs.pop(name)))
        query: list[str] = []
        for name in list(kwargs):  # query 参数拼 query string
            if name in self._query_params():
                val = kwargs.pop(name)
                if isinstance(val, bool):
                    val = "true" if val else "false"  # JSON Schema boolean 字面量
                else:
                    val = str(val)
                encoded = urllib.parse.quote(val)
                query.append(f"{name}={encoded}")
        if query:
            url += "?" + "&".join(query)
        request_headers = dict(self.headers)
        for name in list(kwargs):  # header 参数（in: header）进请求头，不进 body
            if name in self._header_params():
                request_headers[name] = str(kwargs.pop(name))
        body: bytes | None = None
        if self.method in ("post", "put", "patch") and kwargs:
            body = json.dumps(kwargs).encode("utf-8")

        req = urllib.request.Request(
            url, data=body, method=self.method.upper(), headers=request_headers
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        # 5xx/网络错误重试（最多 retries 次额外尝试）；4xx 不重试。
        # 错误统一抛 ValueError（含 code/body 摘要），由 BaseTool.invoke
        # 转 Error 字符串回传 agent（避免 HTTP/网络两种错误形态分叉）。
        last: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                with opener.open(req, timeout=self.timeout_s) as r:
                    raw = r.read().decode("utf-8", "replace")
                break
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", "replace")
                if exc.code >= 500 and attempt < self.retries:
                    last = exc
                    continue
                raise ValueError(f"HTTP {exc.code}: {detail[:200]}") from exc
            except (urllib.error.URLError, TimeoutError) as exc:
                last = exc
                if attempt < self.retries:
                    continue
                raise ValueError(f"HTTP 请求失败：{exc}") from exc
        else:
            assert last is not None
            if isinstance(last, urllib.error.HTTPError):
                raise ValueError(
                    f"HTTP {last.code}: 重试 {self.retries} 次仍失败"
                ) from last
            raise ValueError(f"HTTP 请求失败：{last}") from last
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw