# ReactiveGraph CLI

The `reactivegraph-cli` distribution provides the `reactivegraph` console
command for validating projects, running a local Driver, exporting traces, and
generating deployment artifacts.

## Install

```bash
pip install reactivegraph-cli
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
