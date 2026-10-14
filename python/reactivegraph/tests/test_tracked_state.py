"""TrackedStateProxy tests — mirror of packages/driver/test/state/transaction.test.ts.

The shared patch corpus asserts canonical path encoding, mutation patches, and
pre-image hashes behave identically to the TypeScript store.
"""

from __future__ import annotations

from reactivegraph.state import (
    TrackedState,
    TrackedStateProxy,
    canonical_hash,
    parse_path,
    path_to_string,
)


class TestPathCodec:
    def test_round_trips_canonical_paths(self) -> None:
        cases = [
            ("", ()),
            ("a", ("a",)),
            ("a.b", ("a", "b")),
            ("a[0]", ("a", 0)),
            ("a.b[1].c", ("a", "b", 1, "c")),
            ("a\\.b", ("a.b",)),
        ]
        for canonical, segs in cases:
            assert path_to_string(segs) == canonical
            assert parse_path(canonical) == segs


class TestTrackedProxy:
    def test_nested_reads_recorded(self) -> None:
        proxy = TrackedStateProxy({"a": {"b": {"c": 1}}})
        _ = proxy["a"]["b"]["c"]
        assert "a.b.c" in proxy.read_paths()
        assert "a.b" in proxy.read_paths()
        assert "a" in proxy.read_paths()

    def test_mutation_records_patch(self) -> None:
        proxy = TrackedStateProxy({"a": {"b": 1}})
        proxy["a"]["b"] = 2
        patches = proxy.patches()
        assert len(patches) == 1
        assert patches[0]["operation"] == "set"
        assert patches[0]["path"] == ("a", "b")
        assert patches[0]["beforeHash"] == canonical_hash(1)

    def test_list_slice_read(self) -> None:
        """list 值经代理读取时支持切片（任务内 `s["items"][1:3]` 常见）。"""
        proxy = TrackedStateProxy({"items": [10, 20, 30, 40]})
        items = proxy["items"]
        assert len(items) == 4
        assert items[2] == 30
        assert items[1:3] == [20, 30]  # 切片：返回普通 list
        assert items[:2] == [10, 20]
        assert items[2:] == [30, 40]
        assert "items" in proxy.read_paths()

    def test_proxy_repr_shows_content(self) -> None:
        """repr 显示内容而非对象地址（模板拼接/日志不产出垃圾）。"""
        proxy = TrackedStateProxy({"a": {"b": [1, 2]}})
        assert repr(proxy) == repr({"a": {"b": [1, 2]}})
        assert "TrackedStateProxy" not in repr(proxy)
        # 子代理（host 模式任务内 `s["tool_result"]` 形态）同样可读
        child = proxy["a"]
        assert repr(child) == repr({"b": [1, 2]})

    def test_branch_switch_dependency_cleanup(self) -> None:
        proxy = TrackedStateProxy({"flag": False, "x": 1, "y": 2})
        _ = proxy["flag"]
        # branch not taken -> no read of y
        assert "y" not in proxy.read_paths()
        assert "flag" in proxy.read_paths()

    def test_array_and_map_mutation(self) -> None:
        proxy = TrackedStateProxy({"list": [1, 2, 3], "m": {"k": "v"}})
        proxy["list"].append(4)
        proxy["m"]["k2"] = "v2"
        patches = proxy.patches()
        assert any(p["operation"] == "append" for p in patches)
        assert any(p["operation"] == "set" and p["path"] == ("m", "k2") for p in patches)

    def test_snapshot_is_plain_dict(self) -> None:
        proxy = TrackedStateProxy({"a": {"b": [1, 2]}})
        snap = proxy.snapshot()
        assert isinstance(snap, dict)
        assert not isinstance(snap["a"], TrackedStateProxy)
        assert snap == {"a": {"b": [1, 2]}}
        # mutating the snapshot must not touch the proxy
        snap["a"]["b"].append(99)
        assert proxy.snapshot()["a"]["b"] == [1, 2]

    def test_rollback_restores_committed_state(self) -> None:
        state = TrackedState({"a": 1})
        state.begin()
        state.proxy["a"] = 100
        assert state.proxy["a"] == 100
        state.rollback()
        assert state.state()["a"] == 1

    def test_missing_hash_matches_ts_marker(self) -> None:
        from reactivegraph.state import _MISSING, _MISSING_MARKER

        missing_hash = canonical_hash(_MISSING)
        marker_hash = canonical_hash(_MISSING_MARKER)
        assert missing_hash == marker_hash
        assert len(missing_hash) == 64  # sha256 hex


class TestSharedPatchCorpus:
    """Byte-for-byte consistent with the TypeScript patch corpus."""

    # Shared patch corpus (Task 5 step 5): these exact SHA-256 hex digests must
    # match the TS side (packages/driver/test/state/transaction.test.ts).
    CORPUS: list[tuple[object, str]] = [
        (1, "4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a"),
        ("hello", "2b57c5b79a3aee10237006d2fc64b7ecd13b761867f5992f43eda5777a0726d9"),
        ([1, 2, 3], "efd2ce5d1b243784f054828796128a9e3f85044cbfc21f7144a7a448ea3361e6"),
        (
            {"a": 1, "b": [True, None]},
            "781ba872c03932379df02707033108b09a372c8e65767985971101cde7d79c0a",
        ),
        ("rgp:missing", "e6849aec7e0b007792f96dd2ed18e7388696b0d6eca1d01f2a4b1f28d6889351"),
    ]

    def test_hash_of_canonical_values_matches_ts(self) -> None:
        for value, expected in self.CORPUS:
            assert canonical_hash(value) == expected
            # deterministic
            assert canonical_hash(value) == expected

    def test_patch_round_trip_append(self) -> None:
        proxy = TrackedStateProxy({"list": [1, 2, 3]})
        proxy["list"].append(4)
        patches = proxy.patches()
        assert patches[0]["operation"] == "append"
        assert patches[0]["path"] == ("list",)
        assert patches[0]["value"] == 4
        assert patches[0]["beforeHash"] == canonical_hash([1, 2, 3])
        assert proxy.snapshot()["list"] == [1, 2, 3, 4]
