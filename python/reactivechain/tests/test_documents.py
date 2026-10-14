"""M4：文档模型 + 加载器 + 文本切分测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

from reactivechain import ReactiveChainError
from reactivechain.documents import (
    CharacterTextSplitter,
    CSVLoader,
    DirectoryLoader,
    Document,
    JSONLoader,
    RecursiveCharacterTextSplitter,
    SeparatorSplitter,
    TextLoader,
    TokenTextSplitter,
    UrlLoader,
)


def test_document_dataclass() -> None:
    doc = Document("内容", {"k": 1}, id="d1")
    assert doc.to_dict() == {"page_content": "内容", "metadata": {"k": 1}, "id": "d1"}


def test_text_loader(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_text("hello world", encoding="utf-8")
    docs = TextLoader(f).load()
    assert docs[0].page_content == "hello world"
    assert docs[0].metadata["source"] == str(f)


def test_csv_loader(tmp_path: Path) -> None:
    f = tmp_path / "a.csv"
    f.write_text("name,city\n张三,杭州\n李四,北京\n", encoding="utf-8")
    docs = CSVLoader(f).load()
    assert len(docs) == 2
    assert "张三" in docs[0].page_content
    assert docs[0].metadata["city"] == "杭州"


def test_csv_loader_source_column(tmp_path: Path) -> None:
    f = tmp_path / "a.csv"
    f.write_text("name,city\n张三,杭州\n", encoding="utf-8")
    docs = CSVLoader(f, source_column="name").load()
    assert docs[0].page_content == "张三"


def test_json_loader(tmp_path: Path) -> None:
    f = tmp_path / "a.json"
    f.write_text(
        '[{"title": "A", "body": "内容一"}, {"title": "B", "body": "内容二"}]',
        encoding="utf-8",
    )
    docs = JSONLoader(f, content_key="body", metadata_keys=["title"]).load()
    assert [d.page_content for d in docs] == ["内容一", "内容二"]
    assert docs[0].metadata["title"] == "A"


def test_json_loader_bad_file_hint(tmp_path: Path) -> None:
    f = tmp_path / "bad.json"
    f.write_text("{not json", encoding="utf-8")
    with pytest.raises(ReactiveChainError, match="解析失败"):
        JSONLoader(f).load()


def test_url_loader_rejects_bad_scheme() -> None:
    with pytest.raises(ReactiveChainError, match="仅支持 http/https"):
        UrlLoader("ftp://example.com/x").load()


def test_directory_loader(tmp_path: Path) -> None:
    (tmp_path / "sub").mkdir()
    (tmp_path / "a.txt").write_text("甲", encoding="utf-8")
    (tmp_path / "sub" / "b.txt").write_text("乙", encoding="utf-8")
    (tmp_path / "skip.md").write_text("丙", encoding="utf-8")
    docs = DirectoryLoader(tmp_path, glob="**/*.txt").load()
    assert sorted(d.page_content for d in docs) == ["乙", "甲"]


def test_character_splitter() -> None:
    splitter = CharacterTextSplitter(chunk_size=10, chunk_overlap=2)
    chunks = splitter.split_text("aaaa\nbbbb\ncccc\ndddd")
    assert "".join(chunks).replace("\n", "") == "aaaabbbbccccdddd"


def test_recursive_splitter_chinese() -> None:
    text = "第一句。第二句！第三句？第四句。第五句。"
    splitter = RecursiveCharacterTextSplitter(chunk_size=12, chunk_overlap=2)
    chunks = splitter.split_text(text)
    assert len(chunks) >= 2
    assert "第一句" in chunks[0]


def test_recursive_splitter_long_word() -> None:
    text = "x" * 300
    splitter = RecursiveCharacterTextSplitter(chunk_size=100)
    chunks = splitter.split_text(text)
    assert all(len(c) <= 100 for c in chunks)
    assert "".join(chunks) == text


def test_token_splitter_budget() -> None:
    text = "词 " * 400  # 800 字符 ≈ 200 token
    splitter = TokenTextSplitter(chunk_size=50)
    chunks = splitter.split_text(text)
    assert all(len(c) <= 200 for c in chunks)  # 50 token ≈ 200 字符


def test_separator_splitter_keeps_separator() -> None:
    splitter = SeparatorSplitter(chunk_size=100, separator="\n")
    chunks = splitter.split_text("a\nb\nc")
    assert "".join(chunks) == "a\nb\nc"


def test_splitter_invalid_overlap_hint() -> None:
    with pytest.raises(ReactiveChainError, match="chunk_overlap"):
        CharacterTextSplitter(chunk_size=10, chunk_overlap=10)


def test_split_documents_keeps_metadata() -> None:
    doc = Document("第一段。第二段。", {"source": "s1"})
    splitter = RecursiveCharacterTextSplitter(chunk_size=5, chunk_overlap=0)
    out = splitter.split_documents([doc])
    assert all(d.metadata["source"] == "s1" for d in out)
    assert len(out) >= 2


def test_directory_loader_skips_symlink_escape(tmp_path: Path) -> None:
    """审查反馈：符号链接指向目录外文件时跳过（防任意文件读取）。"""
    inside = tmp_path / "root"
    inside.mkdir()
    (inside / "ok.txt").write_text("内部", encoding="utf-8")
    outside = tmp_path / "outside.txt"
    outside.write_text("机密", encoding="utf-8")
    (inside / "link.txt").symlink_to(outside)
    loader = DirectoryLoader(inside, glob="**/*.txt")
    docs = list(loader.lazy_load())
    contents = [d.page_content for d in docs]
    assert "内部" in contents
    assert "机密" not in contents  # symlink 逃逸被跳过


def test_url_loader_enforces_max_bytes() -> None:
    """审查反馈：URL 响应超过 max_bytes 抛 ReactiveChainError。"""
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class _BigHandler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # silence
            pass

        def do_GET(self) -> None:  # noqa: N802
            body = ("x" * 5000).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _BigHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/big"
        loader = UrlLoader(url, max_bytes=100)
        with pytest.raises(ReactiveChainError, match="超过上限"):
            list(loader.lazy_load())
        # 未超限时正常加载
        big_loader = UrlLoader(url, max_bytes=10_000)
        assert list(big_loader.lazy_load())[0].page_content == "x" * 5000
    finally:
        server.shutdown()