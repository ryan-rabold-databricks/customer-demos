"""<env>_phi_framework_setup: idempotent DDL, keys, constraints, comments, grants, and demo data.

Runs on every deployment. Bundles do not manage Delta tables, so this task creates the governance
tables and brings their constraints and comments up to date. With --seed_demo true it also creates
two synthetic domain schemas and registers them in classification_scope_registry. The synthetic
data contains no real PHI.
"""
import logging
import sys
import time

# Make the bundle's src directory importable; the job passes --src_root ${workspace.file_path}/src.
if "--src_root" in sys.argv:
    sys.path.insert(0, sys.argv[sys.argv.index("--src_root") + 1])

from phi_framework import config, ddl  # noqa: E402
from phi_framework import constants as c  # noqa: E402
from phi_framework.logic import quote_ident, quote_literal  # noqa: E402
from phi_framework.runtime import configure_logging, get_spark  # noqa: E402

log = logging.getLogger("phi.setup")

DEMO_SCHEMAS = ("phi_demo_clinical", "phi_demo_billing")


def _extra(p):
    p.add_argument("--app_sp_client_id", default="", help="Review app service principal to grant.")
    p.add_argument("--seed_demo", default="false", choices=("true", "false"))
    p.add_argument("--clinical_steward_group", default="")
    p.add_argument("--billing_steward_group", default="")
    p.add_argument("--governance_owner", default="")
    p.add_argument("--warehouse_id", default="", help="SQL warehouse for the demo materialized view.")


def run_all(spark, statements):
    for stmt in statements:
        spark.sql(stmt)


def existing_state(spark, cfg) -> ddl.ExistingState:
    """Read deployed comments, properties, and key constraints for the governance tables."""
    cat, sch = quote_literal(cfg.catalog), quote_literal(cfg.governance_schema)
    info = f"`{cfg.catalog}`.information_schema"
    state = ddl.ExistingState()
    for r in spark.sql(f"SELECT table_name, comment FROM {info}.tables WHERE table_schema = {sch}").collect():
        state.table_comments[r.table_name] = r.comment
    for r in spark.sql(
        f"SELECT table_name, column_name, comment FROM {info}.columns WHERE table_schema = {sch}"
    ).collect():
        state.column_comments[(r.table_name, r.column_name)] = r.comment
    for r in spark.sql(
        f"SELECT constraint_name FROM {info}.table_constraints "
        f"WHERE table_catalog = {cat} AND table_schema = {sch} AND constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')"
    ).collect():
        state.key_constraints.add(r.constraint_name)
    for t in ddl.TABLES:
        props = spark.sql(f"SHOW TBLPROPERTIES {cfg.table(t.name)}").collect()
        state.properties[t.name] = {r.key: r.value for r in props}
    return state


def grant_app(spark, cfg, client_id: str) -> None:
    principal = quote_ident(client_id)
    run_all(spark, [
        f"GRANT USE SCHEMA, SELECT ON SCHEMA {cfg.schema_fqn} TO {principal}",
        f"GRANT MODIFY ON TABLE {cfg.table('classification_decision_submission')} TO {principal}",
    ])
    log.info("Granted review app %s read access and submission MODIFY.", client_id)


def _run_on_warehouse(warehouse_id: str, statement: str) -> None:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient()
    resp = w.statement_execution.execute_statement(statement=statement, warehouse_id=warehouse_id, wait_timeout="50s")
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"Warehouse statement failed: {resp.status.error}")


def seed_demo(spark, cfg, args) -> None:
    cat = quote_ident(cfg.catalog)
    clinical, billing = (f"{cat}.{quote_ident(s)}" for s in DEMO_SCHEMAS)
    run_all(spark, [
        f"CREATE SCHEMA IF NOT EXISTS {clinical} COMMENT 'Synthetic clinical domain for the PHI framework demo.'",
        f"CREATE SCHEMA IF NOT EXISTS {billing} COMMENT 'Synthetic billing domain for the PHI framework demo.'",
        f"""CREATE TABLE IF NOT EXISTS {clinical}.patient_encounter (
              encounter_id STRING COMMENT 'Encounter identifier',
              patient_name STRING COMMENT 'Patient legal name',
              mrn STRING COMMENT 'Medical record number',
              birth_date DATE COMMENT 'Patient date of birth',
              patient_email STRING COMMENT 'Patient email address',
              admit_ts TIMESTAMP COMMENT 'Admission time',
              source_system_code STRING COMMENT 'Source EHR system code',
              diagnosis_code STRING COMMENT 'ICD-10 diagnosis code')
            TBLPROPERTIES ('delta.enableRowTracking' = 'true')""",
        f"""CREATE TABLE IF NOT EXISTS {billing}.claim (
              claim_id STRING COMMENT 'Claim identifier',
              member_name STRING COMMENT 'Health plan member name',
              member_ssn STRING COMMENT 'Member Social Security number',
              health_plan_member_id STRING COMMENT 'Health plan beneficiary number',
              claim_amount DECIMAL(12,2) COMMENT 'Billed amount',
              payer_code STRING COMMENT 'Payer reference code')""",
    ])
    if spark.sql(f"SELECT count(*) FROM {clinical}.patient_encounter").first()[0] == 0:
        spark.sql(f"""
            INSERT INTO {clinical}.patient_encounter
            SELECT concat('ENC-', lpad(id, 5, '0')), concat('Synthetic Patient ', id),
                   concat('MRN', lpad(id * 7919 % 100000, 6, '0')), date_add(date'1950-01-01', CAST(id * 397 % 25000 AS INT)),
                   concat('patient', id, '@example.org'), timestamp'2026-09-01 08:00:00' + make_interval(0, 0, 0, CAST(id % 30 AS INT), CAST(id % 24 AS INT)),
                   element_at(array('EPIC', 'CERNER', 'MEDITECH'), id % 3 + 1),
                   element_at(array('E11.9', 'I10', 'J45.909', 'M54.5'), id % 4 + 1)
            FROM (SELECT CAST(id AS INT) AS id FROM range(1, 51))""")
    if spark.sql(f"SELECT count(*) FROM {billing}.claim").first()[0] == 0:
        spark.sql(f"""
            INSERT INTO {billing}.claim
            SELECT concat('CLM-', lpad(id, 6, '0')), concat('Synthetic Member ', id),
                   concat('900-', lpad(id % 100, 2, '0'), '-', lpad(id * 37 % 10000, 4, '0')),
                   concat('HPM', lpad(id * 104729 % 1000000, 7, '0')),
                   CAST(100 + id * 13.37 AS DECIMAL(12,2)), element_at(array('PAYER-A', 'PAYER-B'), id % 2 + 1)
            FROM (SELECT CAST(id AS INT) AS id FROM range(1, 51))""")
    if args.warehouse_id:
        # Materialized views are created on a SQL warehouse; serverless job compute cannot create them.
        _run_on_warehouse(args.warehouse_id, f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {clinical}.encounter_daily_summary
            COMMENT 'Daily encounter counts by source system'
            AS SELECT date(admit_ts) AS admit_date, source_system_code, count(*) AS encounters
               FROM {clinical}.patient_encounter GROUP BY ALL""")

    scopes = [
        ("SCOPE-CLINICAL-" + cfg.env.upper(), DEMO_SCHEMAS[0], "HIGH", args.clinical_steward_group),
        ("SCOPE-BILLING-" + cfg.env.upper(), DEMO_SCHEMAS[1], "STANDARD", args.billing_steward_group),
    ]
    values = ",\n".join(
        f"({quote_literal(sid)}, {quote_literal(cfg.catalog)}, {quote_literal(schema)}, "
        f"{quote_literal(cfg.env)}, {quote_literal(tier)}, {quote_literal(group)}, "
        f"{quote_literal(args.governance_owner)}, true, date'2026-10-01', CAST(NULL AS DATE))"
        for sid, schema, tier, group in scopes
    )
    # Data Governance owns the registry; seeded rows are recorded as an approved MANUAL change.
    spark.sql(f"""
        MERGE INTO {cfg.table('classification_scope_registry')} t
        USING (SELECT * FROM VALUES {values}
               AS s(scope_id, catalog_name, schema_name, environment, risk_tier, steward_group,
                    governance_owner, scan_enabled, effective_date, expiration_date)) s
        ON t.scope_id = s.scope_id
        WHEN NOT MATCHED THEN INSERT (
          scope_id, catalog_name, schema_name, environment, risk_tier, steward_group, governance_owner,
          scan_enabled, effective_date, expiration_date,
          audit_created_at, audit_created_by, audit_created_process, audit_created_run_id,
          audit_updated_at, audit_updated_by, audit_updated_process, audit_updated_run_id)
        VALUES (
          s.scope_id, s.catalog_name, s.schema_name, s.environment, s.risk_tier, s.steward_group,
          s.governance_owner, s.scan_enabled, s.effective_date, s.expiration_date,
          current_timestamp(), current_user(), '{c.MANUAL}', NULL,
          current_timestamp(), current_user(), '{c.MANUAL}', NULL)""")
    log.info("Demo schemas and scope rows are in place.")


def main(argv=None):
    configure_logging()
    cfg, args = config.parse(__doc__, argv, _extra)
    spark = get_spark()
    run_all(spark, ddl.create_statements(cfg))
    statements = ddl.maintenance_statements(cfg, existing_state(spark, cfg))
    log.info("Applying %d maintenance statements.", len(statements))
    run_all(spark, statements)
    log.info("Governance tables, keys, constraints, and comments are up to date.")
    if args.app_sp_client_id:
        grant_app(spark, cfg, args.app_sp_client_id)
    if args.seed_demo == "true":
        if not (args.clinical_steward_group and args.billing_steward_group and args.governance_owner):
            raise ValueError("--seed_demo requires steward groups and --governance_owner")
        seed_demo(spark, cfg, args)


if __name__ == "__main__":
    main()
