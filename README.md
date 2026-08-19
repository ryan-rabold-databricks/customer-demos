# customer-demos

A collection of self-contained, deployable Databricks demos and reference bundles that can be
shared with customers. Each subdirectory is an independent
[Declarative Automation Bundle (DAB)](https://docs.databricks.com/dev-tools/bundles/) with its own
README, deploy steps, and prerequisites.

## Demos

| Demo | What it shows |
|------|---------------|
| [`domain-tag-propagation/`](domain-tag-propagation/) | Automates Databricks **Discover data domains** driven from Unity Catalog **governed tags**: reads an approved domain roster from a master governed tag, ensures each domain's governed tag key and Discover domain card exist, then propagates per-domain tags from schemas down to their tables, views, materialized views, streaming tables, metric views, and volumes. Idempotent and dry-run capable. |

## Using a demo

Each demo is a standalone bundle. In general:

```bash
cd <demo-directory>

# Point the bundle at your own workspace: edit workspace.profile in databricks.yml,
# or set it to a profile from `databricks auth profiles`.

databricks bundle validate --strict --target dev
databricks bundle deploy --target dev
databricks bundle run <resource-name> --target dev
```

See each demo's own `README.md` for parameters, prerequisites, and required privileges.

## Notes

- These are **demonstration** assets. Review each demo's README for caveats — some rely on
  Databricks features or APIs that are in preview or are not yet publicly documented, and those are
  called out per demo.
- All bundles default to a **dry-run / no-write** mode where applicable, so you can preview before
  making changes.
