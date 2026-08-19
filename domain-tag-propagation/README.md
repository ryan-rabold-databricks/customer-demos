# Domain Tag Propagation

Automates the full lifecycle of Databricks **Discover data domains** driven from Unity Catalog
**governed tags**: it reads the allowed domain roster from a master governed tag, ensures each
domain's governed tag key and Discover domain card exist, then propagates per-domain tags from
schemas down to their tables, views, metric views, and volumes so assets surface under the correct
Domain on the Discover page.

Packaged as a **Declarative Automation Bundle (DAB)** with a single serverless job task.

## Why this exists

Manually creating a governed tag key, then a matching Discover domain card, then tagging every
table in a domain, is repetitive and error-prone at scale. This bundle makes the whole flow
declarative, idempotent, and safe to re-run — with a dry-run default so you always preview before
writing.

## The two-tier tag model

This tool deliberately uses **two kinds of governed tag key**. Understanding this is essential:

| Layer | Tag key | Applied to | Applied by | Purpose |
|-------|---------|------------|------------|---------|
| **Master** | e.g. `domain` with allowed-values `[Clinical, Finance, ...]` | **schemas** | data owners (manually, up front) | the allowed-values roster / source of truth |
| **Per-domain** | one key **per allowed value** (e.g. `Clinical`) | **tables/views/metric views/volumes** | this tool | binds each asset to its Discover domain card |

A Discover **domain card** is bound to a per-domain governed tag key. An asset appears under that
domain when it carries the key. Per-domain tags are applied **key-only** (presence markers, no value).

```
master governed tag:  domain = { Clinical, Finance, HR } <- created once by governance
        |
        |  data owner tags a schema:   ALTER SCHEMA ... SET TAG `domain` = `Clinical`
        v
  schema  (domain=Clinical)
        |
        |  this tool, per allowed value:
        |    1. ensure governed tag key `Clinical` (POST /api/2.1/tag-policies)
        |    2. ensure Discover domain card for `Clinical` (POST /api/2.0/domains)
        |    3. tag every in-scope object in the schema with key-only `Clinical`
        v
  table_a, view_b, metric_view_c, ...  (carry `Clinical`)  ->  surface under the "Clinical" domain on Discover
```

## What each run does

1. Reads the allowed values from the master governed tag (default key `domain`).
2. For each allowed value, ensures **both** a governed tag key and a Discover domain card exist.
3. For each schema carrying the master tag, applies the per-domain tag key to every in-scope
   object (tables, views, metric views, volumes — configurable via `object_kinds`).
4. Logs every action per-object and prints a status summary. Idempotent and dry-run capable.

## Prerequisites

- **Databricks CLI** with a configured profile. The `databricks.yml` targets use the `DEFAULT`
  profile — change `workspace.profile` in each target to your own profile (`databricks auth profiles`).
- **Databricks Runtime 16.1+ or current serverless** — required for the `SET TAG ON ...` syntax.
- The **master governed tag must already exist** with its allowed-values roster. Create it once via
  your governance process before running this tool. The tool fails fast if it is missing.
- **Privileges** for the identity running the job:
  - Create/read tag policies (metastore admin, or governed-tag manage permission).
  - Create Discover domains.
  - `APPLY TAG` on the target catalog/schemas/tables.

## Deploy and run

```bash
# From this directory (domain-creation/)

# 1. Validate
databricks bundle validate --strict --target dev

# 2. Deploy the job
databricks bundle deploy --target dev

# 3. Dry run (default: no writes) against a catalog
databricks bundle run domain_tag_propagation --target dev \
  --params catalog_name=<your_catalog>

# 4. Review the printed per-object report, then apply for real
databricks bundle run domain_tag_propagation --target dev \
  --params catalog_name=<your_catalog>,dry_run=false
```

### Parameters

| Variable | Default | Meaning |
|----------|---------|---------|
| `catalog_name` | *(required)* | Catalog to scan and tag. |
| `master_tag_key` | `domain` | Master governed tag key holding the allowed-values roster. |
| `dry_run` | `true` | When `true`, nothing is written. |
| `object_kinds` | `MANAGED,EXTERNAL,VIEW,MATERIALIZED_VIEW,STREAMING_TABLE,METRIC_VIEW,VOLUME` | Comma-separated schema-level object kinds to tag. Valid values: table_type values `MANAGED, EXTERNAL, VIEW, MATERIALIZED_VIEW, STREAMING_TABLE, METRIC_VIEW, FOREIGN` plus `VOLUME`. |

## File structure

```
domain-creation/
├── databricks.yml                              # Bundle config, variables, targets
├── resources/
│   └── domain_tag_propagation.job.yml          # Serverless job with one notebook task
├── src/
│   └── domain_tag_propagation.py               # The notebook (logic)
└── README.md
```

## Design notes and known caveats

- **Additive only.** The tool creates and applies; it never deletes. If a value is removed from the
  master tag, stale domain cards and per-table tags are **not** pruned. Reconciliation is out of scope.
- **Discover domains may be created as drafts.** Depending on the workspace, a newly created domain
  card can land as a draft (`effective_draft = true`) that must be **published in the Discover UI**
  before it is visible. The tool surfaces the draft state per domain (`discover_draft` column) but
  does not auto-publish. Verify the behavior in your workspace.
- **Object-kind coverage.** The executor picks kinds via `object_kinds`:
  - *Tables, views, materialized views, streaming tables* (from `information_schema.tables`):
    tagged with `SET TAG ON TABLE / VIEW / MATERIALIZED VIEW / STREAMING TABLE`.
  - *Metric views* (`table_type = METRIC_VIEW`): tagged with the `SET TAG ON TABLE` keyword (there is
    no `METRIC VIEW` keyword — that is invalid syntax).
  - *Volumes* (from `information_schema.volumes`): `SET TAG ON VOLUME`; idempotency pre-checked via
    `information_schema.volume_tags`.
  Unmapped/unsupported kinds are reported per-object, never fatal.
- **Injection safety.** All read queries use SQL parameter markers; identifiers interpolated into
  DDL are backtick-escaped; widget inputs are pattern-validated.
- **API versions:** governed tags use `/api/2.1/tag-policies` (documented in the public REST API
  reference). Discover data domains use `/api/2.0/domains`, which is **not** currently in the public
  REST API docs — treat it as an undocumented, unstable surface that may change without notice.
