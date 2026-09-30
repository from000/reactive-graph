# ReactiveGraph v0.1.0 pilot invitation

Use this material to invite at least five external users or teams, then record
their results through the `Pilot user` issue template.

## Who we are looking for

Teams evaluating agent/workflow runtimes with at least one real, non-trivial
workload involving tools, retries, durable state, human review, multi-tenant
gateway use, or migration from a state-graph implementation.

## What a pilot includes

- Run one production-like workload on ReactiveGraph.
- Record the environment, graph shape, and observable result.
- Report blockers, incorrect behavior, and missing capabilities.
- Spend roughly 30–60 minutes in a feedback review.
- Allow an anonymized summary to be published, unless the pilot requires a
  stricter confidentiality boundary.

We provide migration guidance, benchmark or deployment review, and direct
maintainer support for the pilot. This is an evaluation invitation, not a paid
support SLA. Do not send credentials, customer data, or other secrets in a
public issue.

## Invitation copy

**Subject:** Pilot ReactiveGraph v0.1.0 with a real agent or workflow

Hello,

We are inviting a small group of teams to pilot ReactiveGraph v0.1.0, a
reactive runtime for explainable and durable agent/workflow execution. We are
looking for one real workload where selective execution, transactional state,
recovery, permissions, or migration from an existing state-graph runtime
matters.

A pilot is intentionally lightweight: run the workload, share the graph shape
and result, and give us a 30–60 minute review of what worked and what blocked
adoption. We will help with migration and deployment questions, prioritize
verified blockers, and summarize findings publicly only with your approval.

If you are interested, open a `Pilot user` issue:
https://github.com/from000/reactive-graph/issues/new?template=pilot_user.md

Thank you,

ReactiveGraph maintainers

## Tracking checklist

- [ ] Identify candidate teams with a concrete workload.
- [ ] Send the invitation with a link to the issue template.
- [ ] Confirm the deployment mode, backend, scale, and data-sharing boundary.
- [ ] Help run the first end-to-end workload.
- [ ] Record blockers and reproducible evidence in the issue.
- [ ] Ask for consent before quoting or publishing a summary.
- [ ] Stop at five verified external pilots only if each result is exposed in
      the public issue or an approved anonymized summary.
