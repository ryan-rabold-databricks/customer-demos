"""Idempotent DDL for the governance tables, keys, constraints, and comments.

Every statement can be rerun: tables use CREATE TABLE IF NOT EXISTS, and constraints are dropped
(IF EXISTS) and re-added so a changed value list takes effect on the next deployment.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Set, Tuple

from . import constants as c
from .config import FrameworkConfig
from .logic import quote_literal


@dataclass(frozen=True)
class Col:
    name: str
    type: str
    not_null: bool
    comment: str


def _audit(created_only: bool, run_id_nullable: bool) -> List[Col]:
    cols = [
        Col("audit_created_at", "TIMESTAMP", True, "When the row was inserted."),
        Col("audit_created_by", "STRING", True, "Identity that inserted the row."),
        Col("audit_created_process", "STRING", True, "Logical process that inserted the row."),
        Col(
            "audit_created_run_id",
            "STRING",
            not run_id_nullable,
            "Lakeflow Job run that inserted the row; NULL for manual or app writes.",
        ),
    ]
    if not created_only:
        cols += [
            Col("audit_updated_at", "TIMESTAMP", True, "When the row was last changed."),
            Col("audit_updated_by", "STRING", True, "Identity that last changed the row."),
            Col("audit_updated_process", "STRING", True, "Logical process that last changed the row."),
            Col(
                "audit_updated_run_id",
                "STRING",
                not run_id_nullable,
                "Lakeflow Job run that last changed the row; NULL for manual writes.",
            ),
        ]
    return cols


@dataclass(frozen=True)
class TableDef:
    name: str
    comment: str
    columns: Tuple[Col, ...]
    pk: str
    append_only: bool = False
    cluster_by: Optional[str] = None


SCOPE_REGISTRY = TableDef(
    "classification_scope_registry",
    "Defines which catalogs and schemas the weekly scan covers. Maintained by Data Governance.",
    (
        Col("scope_id", "STRING", True, "Business identifier assigned by Data Governance."),
        Col("catalog_name", "STRING", True, "Unity Catalog catalog included in the scope."),
        Col("schema_name", "STRING", True, "Schema name, or * for every schema in the catalog."),
        Col("environment", "STRING", True, "Deployment environment the scope belongs to."),
        Col("risk_tier", "STRING", True, "Governance risk level that sets the recertification interval."),
        Col("steward_group", "STRING", True, "Group authorized to review the scoped domain."),
        Col("governance_owner", "STRING", True, "Data Governance owner accountable for the scope."),
        Col("scan_enabled", "BOOLEAN", True, "Whether the scheduled scan includes the scope."),
        Col("effective_date", "DATE", True, "Date on which the scope becomes active."),
        Col("expiration_date", "DATE", False, "Optional date on which the scope stops being active."),
        *_audit(created_only=False, run_id_nullable=True),
    ),
    pk="scope_id",
)

REVIEW_BATCH = TableDef(
    "classification_review_batch",
    "One review package per scope per scan run.",
    (
        Col("review_batch_id", "STRING", True, "Deterministic key: make_key('BATCH', scan_run_id, scope_id)."),
        Col("scope_id", "STRING", True, "Scope the batch covers."),
        Col("scan_run_id", "STRING", True, "Scan job run that created the batch."),
        Col("status", "STRING", True, "OPEN or CLOSED."),
        Col("created_at", "TIMESTAMP", True, "When the scan created the batch."),
        Col("due_at", "TIMESTAMP", True, "When steward review is due."),
        Col("closed_at", "TIMESTAMP", False, "When every item in the batch was resolved."),
        Col("last_error", "STRING", False, "Most recent batch-level error, if one occurred."),
        *_audit(created_only=False, run_id_nullable=False),
    ),
    pk="review_batch_id",
)

REVIEW_ITEM = TableDef(
    "classification_review_item",
    "One record per column requiring classification review or remediation.",
    (
        Col("review_item_id", "STRING", True, "Deterministic key for the column review item."),
        Col("review_batch_id", "STRING", True, "Review batch containing the item."),
        Col("catalog_name", "STRING", True, "Catalog containing the column."),
        Col("schema_name", "STRING", True, "Schema containing the column."),
        Col("table_name", "STRING", True, "Table containing the column."),
        Col("table_type", "STRING", True, "Unity Catalog object type when scanned."),
        Col("column_name", "STRING", True, "Column under review."),
        Col("full_data_type", "STRING", True, "Current Unity Catalog full data type."),
        Col("column_comment", "STRING", False, "Current Unity Catalog column comment."),
        Col("column_fingerprint", "STRING", True, "SHA-256 of normalized identity, full data type, and comment."),
        Col("issue_type", "STRING", True, "Reason the scan opened the item."),
        Col("current_tag_value", "STRING", False, "Direct tag value on the column when scanned."),
        Col("suggested_tag_value", "STRING", False, "Suggested governed-tag value, when one exists."),
        Col("suggestion_reason", "STRING", False, "Source of the suggestion."),
        Col("assigned_steward", "STRING", False, "Data Steward accountable for the item."),
        Col("decision", "STRING", False, "The accepted steward decision."),
        Col("corrected_tag_value", "STRING", False, "Replacement value when the steward corrects the suggestion."),
        Col("steward_comment", "STRING", False, "Steward rationale or supporting comment."),
        Col("reviewed_by", "STRING", False, "Authenticated identity that submitted the decision in the review app."),
        Col("reviewed_at", "TIMESTAMP", False, "When the decision was submitted in the review app (UTC)."),
        Col("status", "STRING", True, "Workflow status."),
        Col("applied_at", "TIMESTAMP", False, "When the planned action completed."),
        Col("application_error", "STRING", False, "Application or verification error, when present."),
        *_audit(created_only=False, run_id_nullable=False),
    ),
    pk="review_item_id",
    cluster_by="review_batch_id",
)

APPLICATION_PLAN = TableDef(
    "classification_application_plan",
    "Validated tag actions. Append-only: never updated or deleted.",
    (
        Col("application_plan_id", "STRING", True, "Deterministic key: make_key('PLAN', review_item_id)."),
        Col("review_batch_id", "STRING", True, "Batch that authorized the action."),
        Col("review_item_id", "STRING", True, "Review item that produced the action."),
        Col("action_type", "STRING", True, "SET_PHI_TAG or REMOVE_PHI_TAG."),
        Col("catalog_name", "STRING", True, "Target catalog captured when the plan was created."),
        Col("schema_name", "STRING", True, "Target schema captured when the plan was created."),
        Col("table_name", "STRING", True, "Target table captured when the plan was created."),
        Col("table_type", "STRING", True, "Target object type; revalidated before execution."),
        Col("column_name", "STRING", True, "Target column captured when the plan was created."),
        Col("column_fingerprint", "STRING", True, "Expected fingerprint, revalidated before execution."),
        Col("approved_tag_value", "STRING", False, "Approved value; NULL for REMOVE_PHI_TAG."),
        Col("reviewed_by", "STRING", True, "Steward identity carried into the plan."),
        Col("reviewed_at", "TIMESTAMP", True, "Steward decision time carried into the plan."),
        Col("rendered_ddl", "STRING", True, "Exact validated tag statement."),
        *_audit(created_only=True, run_id_nullable=False),
    ),
    pk="application_plan_id",
    append_only=True,
)

ATTESTATION = TableDef(
    "classification_attestation",
    "Evidence that a column is not PHI. Superseded when a newer decision replaces it.",
    (
        Col("attestation_id", "STRING", True, "Deterministic key: make_key('ATT', review_item_id)."),
        Col("review_batch_id", "STRING", True, "Batch that authorized the attestation."),
        Col("review_item_id", "STRING", True, "Review item the attestation resolved."),
        Col("catalog_name", "STRING", True, "Catalog containing the attested column."),
        Col("schema_name", "STRING", True, "Schema containing the attested column."),
        Col("table_name", "STRING", True, "Table containing the attested column."),
        Col("column_name", "STRING", True, "Column confirmed not to contain PHI."),
        Col("column_fingerprint", "STRING", True, "Fingerprint the attestation applies to."),
        Col("attestation_type", "STRING", True, "Governed non-PHI attestation outcome."),
        Col("rationale", "STRING", True, "Steward's reason the column is not PHI."),
        Col("attested_by", "STRING", True, "Steward who made the decision."),
        Col("attested_at", "TIMESTAMP", True, "When ingestion recorded the attestation."),
        Col("expires_at", "TIMESTAMP", True, "attested_at plus the scope's recertification interval."),
        Col("superseded_at", "TIMESTAMP", False, "When a newer decision replaced this attestation."),
        *_audit(created_only=False, run_id_nullable=False),
    ),
    pk="attestation_id",
)

REVIEW_EVENT = TableDef(
    "classification_review_event",
    "Workflow audit trail. Append-only: never updated or deleted.",
    (
        Col("event_id", "STRING", True, "Deterministic key for the event."),
        Col("review_batch_id", "STRING", True, "Batch the event relates to."),
        Col("review_item_id", "STRING", False, "Review item the event relates to, when applicable."),
        Col("event_type", "STRING", True, "What happened."),
        Col("event_timestamp", "TIMESTAMP", True, "When it happened."),
        Col("business_actor", "STRING", False, "Person responsible for the underlying decision."),
        Col("event_details", "VARIANT", False, "Structured details specific to the event type."),
        Col("discriminator", "STRING", False, "Source submission_id for ingestion events; NULL otherwise."),
        *_audit(created_only=True, run_id_nullable=False),
    ),
    pk="event_id",
    append_only=True,
)

DECISION_SUBMISSION = TableDef(
    "classification_decision_submission",
    "Decisions and assignments submitted through the review app. Append-only inbox read by ingestion.",
    (
        Col("submission_id", "STRING", True, "Deterministic key for the submission."),
        Col("submission_type", "STRING", True, "DECISION or ASSIGNMENT."),
        Col("review_batch_id", "STRING", True, "Batch of the item the submission targets."),
        Col("review_item_id", "STRING", True, "Item the submission targets."),
        Col("decision", "STRING", False, "Submitted decision, for DECISION submissions."),
        Col("corrected_tag_value", "STRING", False, "Submitted corrected value."),
        Col("steward_comment", "STRING", False, "Submitted rationale or comment."),
        Col("assigned_steward", "STRING", False, "Steward to assign, for ASSIGNMENT submissions."),
        Col("submitted_by", "STRING", True, "Authenticated identity captured by the review app."),
        Col("submitter_role", "STRING", True, "Role the app verified for the submitter."),
        Col("submitted_at", "TIMESTAMP", True, "When the app received the submission (UTC)."),
        *_audit(created_only=True, run_id_nullable=True),
    ),
    pk="submission_id",
    append_only=True,
)

TABLES = (
    SCOPE_REGISTRY,
    REVIEW_BATCH,
    REVIEW_ITEM,
    APPLICATION_PLAN,
    ATTESTATION,
    REVIEW_EVENT,
    DECISION_SUBMISSION,
)

FOREIGN_KEYS = (
    ("classification_review_batch", "fk_batch_scope", "scope_id", "classification_scope_registry", "scope_id"),
    ("classification_review_item", "fk_item_batch", "review_batch_id", "classification_review_batch", "review_batch_id"),
    ("classification_application_plan", "fk_plan_batch", "review_batch_id", "classification_review_batch", "review_batch_id"),
    ("classification_application_plan", "fk_plan_item", "review_item_id", "classification_review_item", "review_item_id"),
    ("classification_attestation", "fk_att_batch", "review_batch_id", "classification_review_batch", "review_batch_id"),
    ("classification_attestation", "fk_att_item", "review_item_id", "classification_review_item", "review_item_id"),
    ("classification_review_event", "fk_event_batch", "review_batch_id", "classification_review_batch", "review_batch_id"),
    ("classification_review_event", "fk_event_item", "review_item_id", "classification_review_item", "review_item_id"),
    ("classification_decision_submission", "fk_sub_item", "review_item_id", "classification_review_item", "review_item_id"),
)


def _in_list(values: Sequence[str]) -> str:
    return ", ".join(quote_literal(v) for v in values)


def check_constraints(cfg: FrameworkConfig) -> List[Tuple[str, str, str]]:
    """(table, constraint_name, expression) for every controlled-value CHECK."""
    jobs = cfg.job_names
    checks = [
        ("classification_scope_registry", "valid_environment", f"environment IN ({_in_list(c.ENVIRONMENTS)})"),
        ("classification_scope_registry", "valid_risk_tier", f"risk_tier IN ({_in_list(tuple(c.RISK_TIER_MONTHS))})"),
        ("classification_scope_registry", "catalog_allowed", f"catalog_name IN ({_in_list(cfg.allowed_catalogs)})"),
        ("classification_scope_registry", "governance_schema_excluded",
         f"NOT (catalog_name = {quote_literal(cfg.catalog)} AND schema_name = {quote_literal(cfg.governance_schema)})"),
        ("classification_review_batch", "valid_batch_status", f"status IN ({_in_list(c.BATCH_STATUSES)})"),
        ("classification_review_item", "valid_item_status", f"status IN ({_in_list(c.ITEM_STATUSES)})"),
        ("classification_review_item", "valid_issue_type", f"issue_type IN ({_in_list(c.ISSUE_TYPES)})"),
        ("classification_review_item", "valid_decision", f"decision IS NULL OR decision IN ({_in_list(c.DECISIONS)})"),
        ("classification_review_item", "valid_table_type", f"table_type IN ({_in_list(c.SCANNED_TABLE_TYPES)})"),
        ("classification_application_plan", "valid_action_type", f"action_type IN ({_in_list(c.ACTION_TYPES)})"),
        ("classification_application_plan", "valid_table_type", f"table_type IN ({_in_list(c.SCANNED_TABLE_TYPES)})"),
        ("classification_attestation", "valid_attestation_type", f"attestation_type IN ({_in_list(c.ATTESTATION_TYPES)})"),
        ("classification_review_event", "valid_event_type", f"event_type IN ({_in_list(c.EVENT_TYPES)})"),
        ("classification_decision_submission", "valid_submission_type", f"submission_type IN ({_in_list(c.SUBMISSION_TYPES)})"),
        ("classification_decision_submission", "valid_submitter_role", f"submitter_role IN ({_in_list(c.SUBMITTER_ROLES)})"),
        ("classification_decision_submission", "valid_submission_decision", f"decision IS NULL OR decision IN ({_in_list(c.DECISIONS)})"),
    ]
    # Process-name rules: MANUAL only on the scope registry, the app only on its inbox,
    # the three framework jobs everywhere else.
    for t in TABLES:
        if t.name == "classification_scope_registry":
            allowed = jobs + (c.MANUAL,)
        elif t.name == "classification_decision_submission":
            allowed = (cfg.app_process_name,)
        else:
            allowed = jobs
        cols = ["audit_created_process"] + ([] if t.append_only else ["audit_updated_process"])
        for col in cols:
            checks.append((t.name, f"valid_{col}", f"{col} IN ({_in_list(allowed)})"))
    return checks


def create_statements(cfg: FrameworkConfig) -> List[str]:
    stmts = [f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_fqn}"]
    for t in TABLES:
        cols = ",\n  ".join(
            f"{col.name} {col.type}{' NOT NULL' if col.not_null else ''} COMMENT {quote_literal(col.comment)}"
            for col in t.columns
        )
        props = ["'delta.enableDeletionVectors' = 'true'"]
        if t.append_only:
            props.append("'delta.appendOnly' = 'true'")
        cluster = f"\nCLUSTER BY ({t.cluster_by})" if t.cluster_by else ""
        stmts.append(
            f"CREATE TABLE IF NOT EXISTS {cfg.table(t.name)} (\n  {cols}\n)"
            f"{cluster}\nCOMMENT {quote_literal(t.comment)}\nTBLPROPERTIES ({', '.join(props)})"
        )
    return stmts


@dataclass
class ExistingState:
    """What is already deployed, so maintenance only issues statements that change something."""

    table_comments: Dict[str, Optional[str]] = field(default_factory=dict)
    column_comments: Dict[Tuple[str, str], Optional[str]] = field(default_factory=dict)
    properties: Dict[str, Dict[str, str]] = field(default_factory=dict)
    key_constraints: Set[str] = field(default_factory=set)


def maintenance_statements(cfg: FrameworkConfig, existing: Optional[ExistingState] = None) -> List[str]:
    """Statements that bring a deployment up to date: comments, properties, keys, CHECKs, make_key.

    With `existing` empty every statement is issued, which is always safe to rerun.
    """
    existing = existing or ExistingState()
    stmts: List[str] = []
    for t in TABLES:
        props = existing.properties.get(t.name, {})
        if existing.table_comments.get(t.name) != t.comment:
            stmts.append(f"COMMENT ON TABLE {cfg.table(t.name)} IS {quote_literal(t.comment)}")
        if t.append_only and props.get("delta.appendOnly") != "true":
            stmts.append(f"ALTER TABLE {cfg.table(t.name)} SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
        for col in t.columns:
            if existing.column_comments.get((t.name, col.name)) != col.comment:
                stmts.append(
                    f"ALTER TABLE {cfg.table(t.name)} ALTER COLUMN {col.name} COMMENT {quote_literal(col.comment)}"
                )
    # Informational keys are added only when missing; names are fixed, so a present key is current.
    for t in TABLES:
        if f"pk_{t.name}" not in existing.key_constraints:
            stmts.append(f"ALTER TABLE {cfg.table(t.name)} ADD CONSTRAINT pk_{t.name} PRIMARY KEY ({t.pk})")
    for table, name, col, ref_table, ref_col in FOREIGN_KEYS:
        if name not in existing.key_constraints:
            stmts.append(
                f"ALTER TABLE {cfg.table(table)} ADD CONSTRAINT {name} FOREIGN KEY ({col}) "
                f"REFERENCES {cfg.table(ref_table)} ({ref_col})"
            )
    # CHECKs are replaced when their expression differs, so a changed value list takes effect.
    for table, name, expr in check_constraints(cfg):
        current = existing.properties.get(table, {}).get(f"delta.constraints.{name.lower()}")
        if current == expr:
            continue
        if current is not None or not existing.properties:
            stmts.append(f"ALTER TABLE {cfg.table(table)} DROP CONSTRAINT IF EXISTS {name}")
        stmts.append(f"ALTER TABLE {cfg.table(table)} ADD CONSTRAINT {name} CHECK ({expr})")
    stmts.append(
        f"""CREATE OR REPLACE FUNCTION {cfg.schema_fqn}.make_key(prefix STRING, parts ARRAY<STRING>)
RETURNS STRING
COMMENT 'Deterministic key: prefix plus the first 16 hex characters of SHA-256 over the parts.'
RETURN concat(prefix, '-', upper(substr(
  sha2(array_join(transform(parts, p -> coalesce(p, chr(0))), chr(31)), 256), 1, 16)))"""
    )
    return stmts
