"""文档模型 + 加载器 + 文本切分（对应 langchain_core.documents / text_splitter）。

纯 stdlib 实现：text/csv/json/url/目录加载；html/pdf 留扩展位。
"""

from __future__ import annotations

import csv
import json
import re
import urllib.request
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .runnable import ReactiveChainError

_DEFAULT_METADATA: dict[str, Any] = {}


@dataclass
class Document:
    """检索单元：内容 + 元数据 + 可选 id。"""

    page_content: str
    metadata: dict[str, Any] = field(default_factory=dict)
    id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out = {"page_content": self.page_content, "metadata": dict(self.metadata)}
        if self.id is not None:
            out["id"] = self.id
        return out


class BaseLoader:
    """加载器抽象：`lazy_load()` 产出 Document。"""

    def load(self) -> list[Document]:
        return list(self.lazy_load())

    def lazy_load(self) -> Iterator[Document]:
        raise NotImplementedError  # pragma: no cover


class TextLoader(BaseLoader):
    def __init__(self, file_path: str | Path, *, encoding: str = "utf-8") -> None:
        self.file_path = Path(file_path)
        self.encoding = encoding

    def lazy_load(self) -> Iterator[Document]:
        text = self.file_path.read_text(encoding=self.encoding)
        yield Document(
            page_content=text,
            metadata={"source": str(self.file_path)},
        )


class CSVLoader(BaseLoader):
    """CSV 加载：每行一个 Document，表头进 metadata。"""

    def __init__(
        self,
        file_path: str | Path,
        *,
        encoding: str = "utf-8",
        source_column: str | None = None,
    ) -> None:
        self.file_path = Path(file_path)
        self.encoding = encoding
        self.source_column = source_column

    def lazy_load(self) -> Iterator[Document]:
        with self.file_path.open(newline="", encoding=self.encoding) as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ReactiveChainError(
                    f"CSV 文件 {self.file_path} 无表头 — Hint: 检查文件格式"
                )
            for row in reader:
                content = row.get(self.source_column) if self.source_column else None
                if content is None:
                    content = " ".join(f"{k}: {v}" for k, v in row.items())
                metadata = {k: v for k, v in row.items() if k != self.source_column}
                metadata["source"] = str(self.file_path)
                yield Document(page_content=content, metadata=metadata)


class JSONLoader(BaseLoader):
    """JSON 加载：对象列表（含 JSON Lines）。`jq_schema` 留扩展位。"""

    def __init__(
        self,
        file_path: str | Path,
        *,
        content_key: str | None = None,
        metadata_keys: Sequence[str] = (),
    ) -> None:
        self.file_path = Path(file_path)
        self.content_key = content_key
        self.metadata_keys = list(metadata_keys)

    def lazy_load(self) -> Iterator[Document]:
        raw = self.file_path.read_text(encoding="utf-8")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ReactiveChainError(
                f"JSON 文件 {self.file_path} 解析失败：{exc} — Hint: 检查文件内容"
            ) from exc
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            if self.content_key:
                content = str(item.get(self.content_key, ""))
            else:
                content = json.dumps(item, ensure_ascii=False)
            metadata = {
                k: item.get(k)
                for k in self.metadata_keys
                if k in item and item[k] is not None
            }
            metadata["source"] = str(self.file_path)
            yield Document(page_content=content, metadata=metadata)


class UrlLoader(BaseLoader):
    """URL 加载：GET 拉取文本（纯 stdlib；直接连接的 Web 内容）。

    安全提示：`url` 必须来自可信来源——本加载器不校验目标地址（含内网/
    回环），也不限制重定向，公开暴露前请自行搭配访问控制。
    """

    def __init__(
        self,
        url: str,
        *,
        timeout_s: float = 30.0,
        max_bytes: int = 10 * 1024 * 1024,
        headers: dict[str, str] | None = None,
    ) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ReactiveChainError(
                f"UrlLoader 仅支持 http/https，得到 {parsed.scheme} — Hint: 检查 URL"
            )
        self.url = url
        self.timeout_s = timeout_s
        self.max_bytes = max_bytes
        self.headers = headers or {"User-Agent": "ReactiveChain/0.1"}

    def lazy_load(self) -> Iterator[Document]:
        req = urllib.request.Request(self.url, headers=self.headers)
        # 默认直连（禁系统代理），与 llm/embeddings 网络层行为一致；
        # 避免本地/内网端点被环境代理劫持。
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=self.timeout_s) as r:
            raw = r.read(self.max_bytes + 1)
            if len(raw) > self.max_bytes:
                raise ReactiveChainError(
                    f"URL {self.url} 响应超过上限 {self.max_bytes} 字节 — "
                    f"Hint: 增大 max_bytes 或更换数据源"
                )
            text = raw.decode("utf-8", "replace")
        yield Document(page_content=text, metadata={"source": self.url})


class DirectoryLoader(BaseLoader):
    """目录加载：递归收集文件（白名单扩展名）。"""

    def __init__(
        self,
        path: str | Path,
        *,
        glob: str = "**/*.txt",
        loader_cls: Callable[..., BaseLoader] = TextLoader,
        encoding: str = "utf-8",
    ) -> None:
        self.path = Path(path)
        self.glob = glob
        self.loader_cls = loader_cls
        self.encoding = encoding

    def lazy_load(self) -> Iterator[Document]:
        root = self.path.resolve()
        for fp in sorted(root.glob(self.glob)):
            if not fp.is_file():
                continue
            # 防符号链接逃逸：解析后仍在根目录内的文件才加载。
            resolved = fp.resolve()
            if resolved != root and root not in resolved.parents:
                continue
            loader = self.loader_cls(fp, encoding=self.encoding)
            yield from loader.lazy_load()


class TextSplitter:
    """文本切分器基类：按大小 + 重叠切块。"""

    def __init__(
        self,
        chunk_size: int = 1000,
        chunk_overlap: int = 0,
        *,
        length_fn: Any = len,
    ) -> None:
        if chunk_overlap >= chunk_size:
            raise ReactiveChainError(
                f"chunk_overlap（{chunk_overlap}）必须 < chunk_size（{chunk_size}）— "
                f"Hint: 重叠不能覆盖整个块"
            )
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.length_fn = length_fn

    def split_text(self, text: str) -> list[str]:
        raise NotImplementedError  # pragma: no cover

    def split_documents(self, documents: Sequence[Document]) -> list[Document]:
        out: list[Document] = []
        for doc in documents:
            for chunk in self.split_text(doc.page_content):
                out.append(
                    Document(
                        page_content=chunk,
                        metadata={**doc.metadata, "chunk": len(out)},
                    )
                )
        return out

    def _merge(self, chunks: Sequence[str]) -> list[str]:
        """按 chunk_size 合并基础块，带重叠。"""
        merged: list[str] = []
        current = ""
        for chunk in chunks:
            if current and self.length_fn(current) + 1 + self.length_fn(chunk) > self.chunk_size:
                merged.append(current)
                overlap = self._tail(current, self.chunk_overlap)
                current = overlap + chunk if overlap else chunk
            else:
                current = (current + "\n" + chunk) if current else chunk
        if current:
            merged.append(current)
        return merged

    @staticmethod
    def _tail(text: str, n: int) -> str:
        if n <= 0 or not text:
            return ""
        return text[-n:]


class CharacterTextSplitter(TextSplitter):
    """按分隔符切分（默认换行）。"""

    def __init__(
        self,
        separator: str = "\n\n",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.separator = separator

    def split_text(self, text: str) -> list[str]:
        if not text:
            return []
        parts = text.split(self.separator)
        return self._merge([p for p in parts if p.strip()])


class RecursiveCharacterTextSplitter(TextSplitter):
    """递归分隔符切分：依次尝试常用分隔符。"""

    _SEPARATORS = ["\n\n", "\n", "。", "！", "？", ". ", " ", ""]

    def __init__(self, separators: Sequence[str] | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.separators = list(separators) if separators else list(self._SEPARATORS)

    def split_text(self, text: str) -> list[str]:
        return self._recursive(text, self.separators)

    def _recursive(self, text: str, seps: Sequence[str]) -> list[str]:
        if not seps or self.length_fn(text) <= self.chunk_size:
            return [text] if text.strip() else []
        sep = seps[0]
        if sep == "":
            # 逐字符切块（兜底）
            return [
                text[i : i + self.chunk_size]
                for i in range(0, len(text), self.chunk_size)
                if text[i : i + self.chunk_size].strip()
            ]
        pieces = text.split(sep)
        out: list[str] = []
        for piece in pieces:
            if self.length_fn(piece) <= self.chunk_size:
                if piece.strip():
                    out.append(piece)
            else:
                out.extend(self._recursive(piece, seps[1:]))
        return self._merge(out)


class TokenTextSplitter(RecursiveCharacterTextSplitter):
    """按近似 token 预算切分（4 字符/token；tiktoken 为可选依赖）。"""

    def __init__(self, chunk_size: int = 500, chunk_overlap: int = 0, **kwargs: Any) -> None:
        super().__init__(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_fn=lambda s: max(1, len(s) // 4),
            **kwargs,
        )


class SeparatorSplitter(CharacterTextSplitter):
    """按自定义分隔符切分（保留分隔符为块尾）。"""

    def __init__(self, separator: str = "\n", **kwargs: Any) -> None:
        super().__init__(separator=separator, **kwargs)

    def split_text(self, text: str) -> list[str]:
        if not text:
            return []
        pieces = re.split(rf"({re.escape(self.separator)})", text)
        blocks = [p for p in pieces if p.strip() or p == self.separator]
        if self.separator and len(blocks) > 1:
            blocks = [
                blocks[i] + blocks[i + 1]
                for i in range(0, len(blocks) - 1, 2)
            ] + ([blocks[-1]] if len(blocks) % 2 else [])
        return self._merge_kept_separator([b for b in blocks if b.strip()])

    def _merge_kept_separator(self, chunks: list[str]) -> list[str]:
        """块已自带分隔符，直接拼接（不再插入额外 \\n）。"""
        joined = "".join(chunks)
        if self.length_fn(joined) <= self.chunk_size:
            return [joined] if joined.strip() else []
        return super()._merge(chunks)  # 超长时回退父类合并逻辑