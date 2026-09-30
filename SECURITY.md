# Security Policy

## Reporting a vulnerability

**Preferred:** use GitHub's private vulnerability reporting —
<https://github.com/from000/reactive-graph/security/advisories/new>. This opens
a draft advisory that only the maintainers can see, and it is how a fix gets
prepared and released without disclosing the issue first.

For non-sensitive reports (hardening ideas, dependency notes, missing
validation with no known exploit) a normal issue with the `security` label is
fine.

Please include: affected component (driver / gateway / SDK), reproduction
steps, and impact. We aim to acknowledge within 48h and ship a fix in the next
patch release.

Do not open a public issue for an exploitable vulnerability: that discloses it
before a fix exists.

## Security model

* **RGP/1 gateway authentication** — when `tenants` are configured, tokens map
  to tenants and an unknown token receives an error frame and the session is
  closed before any execution. **When no tenant map is configured (the
  in-process/local default), every token authenticates to the single `default`
  tenant** — that is the explicit no-auth local mode, not a validation gap;
  production deployments must set a tenant map. See docs/spec/remote-api.md.
* **Tenant / run isolation** — sessions carry the authenticated tenant; request
  handlers receive it so multi-tenant deployments can partition runs and
  checkpoints.
* **Field-level permissions** — `PermissionPolicy` + `isPathAllowed`/
  `validatePatches`/`PermissionDeniedError` 提供字段级读写校验原语（含嵌套路径
  与段边界），并有 `controls.test.ts` 覆盖；作为可复用库函数可直接接入补丁
  提交点。注意：当前调度器尚未在生产执行管线中自动强制该校验（known
  limit，见下文），接入是显式调用而非默认行为。
* **State redaction** — sensitive paths (e.g. `credentials`) are replaced by the
  `REDACTED` symbol before trace/span export; configured attribute names are
  redacted at OTel span emission. Raw secrets never leave the process.
* **Checkpoint importer** — the importer is strictly read-only (SQLite
  `mode=ro` URI + a write-probe assertion); it never alters a source database.

## Dependency policy

Third-party runtime dependencies are minimized: the Driver uses
`@vue/reactivity` for state reactivity and the RGP/1 protocol codec; no network
service is required to run local graphs. Postgres/Redis backends and OTLP
collectors are optional and documented as such.

## Known limits

* `pg-store-search`（PostgresStore）、`ckpt-cache-redis`（RedisCache）、
  `cli-up`/`cli-build-dockerfile` 均已实现并经本地服务实测；`pg-shallow-saver`
  已 deprecated，以 PostgresSaver + `durability='exit'` 覆盖。仅全量 OTLP
  导出仍需 collector。
* SDK auth envelope 端到端传输中的服务端校验仍在网关层（gateway 侧 token→tenant
  认证已实现并测试）；SDK 侧 `api_key`/`headers` 已随请求携带（test_auth）。
