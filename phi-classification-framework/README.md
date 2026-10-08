# PHI Classification Framework

A deployable reference implementation of a governed **PHI classification audit process** on Databricks.
A weekly scan finds Unity Catalog columns that lack a direct PHI governed tag, Data Stewards ratify
each finding in a **Databricks App**, and automation applies and verifies the approved tags, keeping
an append-only evidence trail of every decision.

Two outcomes are valid for every in-scope column:

1. The column contains PHI and receives an approved value of the governed tag.
2. The column is confirmed not to contain PHI and receives a documented, time-bound attestation.

A missing tag never means "not PHI"; it means "not yet classified or attested."

## Architecture

```
                         ┌──────────────────────────── <catalog>.<governance_schema> ───────────────────────────┐
 Data Governance ──────► │ classification_scope_registry                                                         │
 (registry rows)         │          │                                                                            │
                         │          ▼                                                                            │
 weekly schedule ──► <env>_phi_classification_scan ──► classification_review_batch / _review_item / _review_event │
 (or correction          │   reads: information_schema (per scoped catalog), column tags, tag policy,            │
  from the app)          │          system.data_classification.results (optional), attestations                  │
                         │          ▼                                                                            │
 Stewards / Governance ──► Review app (FastAPI) ──append──► classification_decision_submission                    │
  (authenticated users)  │                                          │  table-update trigger                       │
                         │                                          ▼                                            │
                         │                       <env>_phi_decision_ingestion ──► _application_plan (append-only)│
                         │                                          │             _attestation, _review_item     │
                         │                                          │  table-update trigger on plan rows         │
                         │                                          ▼                                            │
                         │                       <env>_phi_classification_application ──► SET / UNSET TAGS      │
                         │                                          (verifies in Unity Catalog, closes batches)  │
                         └───────────────────────────────────────────────────────────────────────────────────────┘
```

| Component | Writes | Trigger |
|---|---|---|
| `<env>_phi_framework_setup` | Schema, tables, keys, CHECK constraints, comments, `make_key` function, grants; optional demo data | Run after each deployment |
| `<env>_phi_classification_scan` | Batches, items, events | Weekly schedule (paused in dev); on demand for corrections |
| Review app `phi-review-<env>` | `classification_decision_submission` only | Steward and Governance actions |
| `<env>_phi_decision_ingestion` | Items, plans, attestations, events | New submission rows |
| `<env>_phi_classification_application` | Items, events, Unity Catalog tags | New plan rows |

Design rules carried from the governance process:

- **Only the jobs write governance records.** The app appends to an inbox table; CHECK constraints limit
  each table's audit process column to the framework jobs (the scope registry also accepts `MANUAL`;
  the inbox accepts only the app).
- **Identity is captured, not self-reported.** The app records the authenticated user from the Databricks
  Apps proxy as `reviewed_by` and the submission time as `reviewed_at`. It reads the user's group
  memberships with the forwarded user token to decide who may act; all data access uses the app's
  service principal.
- **Deterministic keys** (`make_key`: SHA-256, 16 hex characters) and insert-only `MERGE` make every job
  safe to rerun. Plans and events are append-only (`delta.appendOnly`).
- **Plans are immutable.** The application job revalidates each column's fingerprint and object type
  before running the exact rendered statement, then polls Unity Catalog to verify the result.
- **Environment guardrail.** Jobs refuse scope rows and plans whose catalog is not in `allowed_catalogs`.

## Prerequisites

- Databricks CLI v0.270 or later (tested with v1.19), authenticated to the target workspace.
- A Unity Catalog catalog where you can create a schema, and a SQL warehouse you can use.
- A **governed tag** whose allowed values are your PHI taxonomy (default key `phi_data_classification`),
  with permission for the job identity to assign it.
- An account group for Data Governance, and steward groups for each scope.
- Python 3.10+ locally for the bootstrap and test scripts: `pip install databricks-sdk pytest`.
- Optional: `SELECT` on `system.data_classification.results` for the scan identity, and Data
  Classification enabled on the scoped catalogs, to get suggested values. Without it the scan logs a
  warning and stewards classify directly.

## Deploy and run

```bash
cd phi-classification-framework

# 1. Point the dev target at your workspace: set targets.dev.workspace.profile and the group
#    variables in databricks.yml, and export the workspace-specific values:
export DATABRICKS_CONFIG_PROFILE=<profile>
export BUNDLE_VAR_catalog=<catalog> BUNDLE_VAR_allowed_catalogs=<catalog> BUNDLE_VAR_warehouse_id=<warehouse-id>

# 2. Create the governance tables once. Table-update triggers require their tables to exist.
python scripts/bootstrap_tables.py --profile <profile> --warehouse_id <warehouse-id> \
  --env dev --catalog <catalog> --allowed_catalogs <catalog>

# 3. Deploy, then run setup (keys, constraints, comments, grants, demo data).
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
databricks bundle run phi_setup -t dev

# 4. Start the review app and run the first scan.
databricks bundle run phi_review_app -t dev
databricks bundle run phi_scan -t dev
```

Open the app URL printed by step 4.

- **My queue** groups open columns by table, explains in plain language why each column is there, and
  shows the reason when a decision was rejected. Select columns to mark them not PHI with one shared
  reason, or to approve their suggested values.
- **Each column** offers three choices: *This is PHI* (pick the value; the suggestion is pre-selected),
  *Not PHI* (reason required), or *Ask Privacy*. *Submit and next* moves to the next column in the queue.
  The page shows the table's other columns and their outcomes, a Catalog Explorer link that opens with
  the user's own permissions, and definitions of each allowed value from the governed tag policy.
- **My decisions** lists everything the user submitted and what happened to it.
- Members of the governance group also see **Data Governance** for assigning stewards, corrections, and
  batch status.

To open a correction outside the app:

```bash
databricks bundle run phi_scan -t dev --params \
  correction_column=<catalog>.<schema>.<table>.<column>,correction_reason="<reason>",correction_requested_by=<email>
```

## Test

```bash
pip install -r src/requirements.txt pytest
pytest tests/                       # workflow rules, app wording, and page renders (no workspace needed)

python scripts/e2e_test.py --profile <profile> --warehouse_id <warehouse-id> \
  --app_url <app-url> --catalog <catalog> --scan_job_id <scan-job-id>
```

The end-to-end test drives the deployed app over HTTP and verifies, in Unity Catalog: assignment,
all four decisions, app-side and ingestion-side validation, triggered ingestion and application, tags
on tables and a materialized view, Privacy review resolution, batch closure, ignored changes to
accepted items, a correction that removes a tag, and detection of orphaned tags and changed metadata.
It modifies only the synthetic demo tables created by `seed_demo`, and expects a freshly seeded
deployment with one completed scan (17 open items); rerun it after dropping the governance and demo schemas
and repeating the deploy steps.

## Configuration

Bundle variables (`databricks.yml`):

| Variable | Purpose | Default |
|---|---|---|
| `env` | `dev`, `test`, or `prod`; prefixes job and audit process names | `dev` |
| `catalog` / `governance_schema` | Location of the governance tables | — / `data_governance` |
| `allowed_catalogs` | Comma-separated catalogs this environment may scan and tag | — |
| `tag_key` | Governed tag key; its allowed values are the taxonomy | `phi_data_classification` |
| `warehouse_id` | SQL warehouse for the app (and the demo materialized view) | — |
| `governance_group` | Group whose members act as Data Governance in the app | — |
| `class_tag_mapping` | JSON map from Data Classification `class_tag` to an allowed value | HIPAA identifier mapping |
| `trigger_pause_status` / `scan_schedule_pause_status` | Pause the triggers or the weekly schedule | `UNPAUSED` / `PAUSED` |
| `seed_demo`, `clinical_steward_group`, `billing_steward_group`, `governance_owner` | Synthetic demo data and scope rows | `false` |

App environment variables are set from these in `resources/review_app.app.yml`.

## Folder structure

```
databricks.yml                 Bundle, variables, dev and prod targets
resources/
  setup.job.yml                Setup job (DDL, constraints, grants, demo data)
  scan.job.yml                 Weekly scan; correction_* job parameters
  ingestion.job.yml            Decision ingestion; table-update trigger on submissions
  application.job.yml          Tag application; table-update trigger on plans
  review_app.app.yml           Databricks App with SQL warehouse and scan-job resources
src/
  phi_framework/               Shared package: constants, keys, pure rules (logic.py), DDL, Spark store
  jobs/                        Job entry points: setup.py, scan.py, ingest.py, apply.py
  review_app/                  FastAPI app: main.py, backend.py, identity.py, templates/, static/
  requirements.txt             App dependencies
scripts/
  bootstrap_tables.py          One-time table creation before the first deploy
  e2e_test.py                  End-to-end test against a dev deployment
tests/test_logic.py            Unit tests
```

## Production notes

- Deploy `test` and `prod` from CI with `mode: production` and `run_as` set to a dedicated service
  principal (see the commented `run_as` in `databricks.yml`). That identity needs `ASSIGN` on the governed
  tag and `APPLY TAG` on the scoped catalogs.
- Set `max_concurrent_runs: 1` (already set) and keep governance tables unpartitioned with deletion
  vectors so concurrent jobs conflict only at row level. Append-only tables are written with blind
  appends (after filtering keys that already exist), because an insert-only `MERGE` reads the whole
  table and conflicts with concurrent appends from another job.
- A task retry keeps the job run ID (verified in dev), so a retried run rebuilds the same keys. If a
  retry finds a submission whose changes committed before its events did, ingestion replays it and
  writes only the missing records.
- Exclude shallow clones, views, and foreign tables (the scan covers `MANAGED`, `EXTERNAL`,
  `STREAMING_TABLE`, and `MATERIALIZED_VIEW`). Pipeline-internal `__materialization_*` and `event_log_*`
  tables are skipped.
- Setup grants the app's service principal `USE SCHEMA` and `SELECT` on the governance schema and
  `MODIFY` on the submission table only; it relies on an existing `USE CATALOG` grant.
- This is a demonstration asset. Review the CHECK constraints, grants, and taxonomy mapping with your
  governance team before production use.
