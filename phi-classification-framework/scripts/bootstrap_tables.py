"""Create the governance schema and tables before the first bundle deploy.

The ingestion and application jobs use table-update triggers, and Databricks rejects a trigger on a
table that does not exist yet. Run this once per environment before the first `bundle deploy`; the
setup job then maintains keys, constraints, and comments on every deployment. Statements come from
the same ddl module the setup job uses, and every statement is idempotent.

Usage:
  python scripts/bootstrap_tables.py --profile <profile> --warehouse_id <id> \
      --env dev --catalog <catalog> --governance_schema data_governance --allowed_catalogs <catalog>
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service.sql import StatementState  # noqa: E402

from phi_framework import ddl  # noqa: E402
from phi_framework.config import FrameworkConfig, job_names  # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", required=True)
    p.add_argument("--warehouse_id", required=True)
    p.add_argument("--env", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--governance_schema", default="data_governance")
    p.add_argument("--allowed_catalogs", required=True)
    a = p.parse_args()
    cfg = FrameworkConfig(a.env, a.catalog, a.governance_schema, "", "", "", tuple(a.allowed_catalogs.split(",")),
                          job_names(a.env), f"{a.env}_phi_review_app")
    w = WorkspaceClient(profile=a.profile)
    for stmt in ddl.create_statements(cfg):
        resp = w.statement_execution.execute_statement(statement=stmt, warehouse_id=a.warehouse_id, wait_timeout="50s")
        while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(2)
            resp = w.statement_execution.get_statement(resp.statement_id)
        if resp.status.state != StatementState.SUCCEEDED:
            raise SystemExit(f"Failed: {stmt.splitlines()[0]}\n{resp.status.error}")
        print("OK:", stmt.splitlines()[0])


if __name__ == "__main__":
    main()
