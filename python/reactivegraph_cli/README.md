# ReactiveGraph CLI

The `reactivegraph-cli` distribution provides the `reactivegraph` console
command for validating projects, running a local Driver, exporting traces, and
generating deployment artifacts.

## Install

尚未发布到 PyPI。上架前请从 GitHub 安装（CLI 依赖
`reactivegraph>=0.1.0`，两个包需要一起装）：

```bash
pip install \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph" \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph_cli"
reactivegraph health
```

`reactivegraph-cli` depends on the `reactivegraph` runtime. The core
`reactivegraph` distribution intentionally does not install a console script.

## Commands

```bash
reactivegraph new my-app
reactivegraph validate my-app
reactivegraph health
reactivegraph dockerfile my-app
reactivegraph up my-app
```

See the [production deployment guide](https://github.com/from000/reactive-graph/blob/main/docs/production-deployment.md)
for container and health-check examples.

## License

MIT — see [LICENSE](https://github.com/from000/reactive-graph/blob/main/LICENSE).
