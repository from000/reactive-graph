# ReactiveGraph root Makefile.
.PHONY: test js-test py-test lint format-check typecheck coverage release-gate

PY := .venv/bin/python

# JavaScript: all workspace package tests via pnpm (may write vitest cache)
js-test:
	pnpm -r test

# Python: native package tests (canonical read-only test entry)
test:
	uv run --directory python/reactivegraph --extra test pytest -q

# Coverage summary (TS driver core + Python native package)
coverage:
	export PYTHONDONTWRITEBYTECODE=1 PYTHONPYCACHEPREFIX=/tmp/rgp-py-cache; \
	pnpm --filter @reactivegraph/driver test --coverage; \
	uv run --directory python/reactivegraph --with pytest --with pytest-asyncio --with msgpack --with pytest-cov \
		pytest -p no:cacheprovider --cov=reactivegraph --cov-report=term

lint:
	pnpm -r lint

format-check:
	pnpm format:check

typecheck:
	pnpm -r typecheck

# Canonical local release gate; phases are serial because Python workspace
# members share one uv-managed .venv.
release-gate:
	scripts/check-release-gate.sh
